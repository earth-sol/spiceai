/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use super::{CatalogConnector, ParameterSpec, Parameters};
use crate::{
    Runtime,
    component::catalog::Catalog,
    dataconnector::{ConnectorComponent, ConnectorParams},
};
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_glue::{
    Client,
    error::SdkError,
    operation::{get_databases::GetDatabasesError, get_tables::GetTablesError},
    types::Table,
};
use aws_sdk_sts::config::{Credentials, ProvideCredentials};
use data_components::RefreshableCatalogProvider;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider, TableProvider},
    common::Result as DFResult,
};
use globset::GlobSet;
use iceberg::{
    NamespaceIdent, TableIdent,
    io::{FileIOBuilder, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY},
    spec::TableMetadata,
    table::Table as IcebergTable,
};
use iceberg_datafusion::IcebergTableProvider;
use snafu::prelude::*;
use std::fmt;
use std::{any::Any, collections::HashMap, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get Glue databases: {}", source))]
    GetDatabases { source: SdkError<GetDatabasesError> },
    #[snafu(display("Failed to get Glue tables: {}", source))]
    GetTables { source: SdkError<GetTablesError> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct GlueCatalog {
    params: Parameters,
}

type DatabaseName = String;

pub struct GlueCatalogProvider {
    inner: Arc<Inner>,
}

impl fmt::Debug for GlueCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueCatalogProvider")
            .finish_non_exhaustive()
    }
}

struct Inner {
    _glue: Client,
    databases: HashMap<DatabaseName, Vec<Table>>,
    config: SdkConfig,
}

pub struct GlueSchemaProvider {
    schema: String,
    inner: Arc<Inner>,
}

impl fmt::Debug for GlueSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueSchemaProvider")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for GlueSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner
            .databases
            .get(&self.schema)
            .map(|tables| tables.iter().map(|t| t.name.clone()).collect())
            .unwrap_or_default()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.databases.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        match self
            .inner
            .databases
            .get(&self.schema)
            .and_then(|tables| tables.iter().find(|t| t.name() == name))
        {
            Some(table) => match TableType::from(table) {
                TableType::HiveParquet => {
                    // TODO:
                    eprintln!("load Hive Parquet table");
                    Ok(None)
                }
                TableType::Iceberg => {
                    let mut props = Vec::new();
                    if let Some(provider) = self.inner.config.credentials_provider() {
                        let creds = provider.provide_credentials().await.unwrap();
                        props.push((S3_ACCESS_KEY_ID, creds.access_key_id().to_string()));
                        props.push((S3_SECRET_ACCESS_KEY, creds.secret_access_key().to_string()));
                    }

                    dbg!(self.inner.config.endpoint_url());

                    let file_io = FileIOBuilder::new("s3").with_props(props).build().unwrap();

                    let metadata_location = get_metadata_location(&table.parameters).unwrap();

                    let input_file = file_io.new_input(&metadata_location).unwrap();
                    let metadata_content = input_file.read().await.unwrap();
                    let metadata =
                        serde_json::from_slice::<TableMetadata>(&metadata_content).unwrap();

                    let identifier =
                        TableIdent::new(NamespaceIdent::new(self.schema.clone()), name.to_string());

                    let table = IcebergTable::builder()
                        .file_io(file_io)
                        .metadata(metadata)
                        .identifier(identifier)
                        .build()
                        .unwrap(); // TODO handle error
                    let table_provider = IcebergTableProvider::try_new_from_table(table)
                        .await
                        .unwrap(); // TODO handle error
                    Ok(Some(Arc::new(table_provider)))
                }
                TableType::Unsupported => Ok(None),
            },
            None => {
                // Should not be able to get here
                tracing::error!("Glue schema name not in databases");
                Ok(None)
            }
        }
    }
}

// copy from iceberg-catalog-glue internals
fn get_metadata_location(parameters: &Option<HashMap<String, String>>) -> Result<String> {
    const METADATA_LOCATION: &str = "metadata_location";
    match parameters {
        Some(properties) => match properties.get(METADATA_LOCATION) {
            Some(location) => Ok(location.to_string()),
            None => panic!("No '{METADATA_LOCATION}' set on table"),
        },
        None => panic!("No 'parameters' set on table. Location of metadata is undefined"),
    }
}

impl GlueCatalogProvider {
    pub async fn new(params: &Parameters, catalog: &Catalog) -> Result<Self> {
        let config = load_config(params).await;
        let glue = Client::new(&config);

        let get_databases_output = glue
            .get_databases()
            .send()
            .await
            .context(GetDatabasesSnafu)?;

        let mut databases = HashMap::new();
        for db in get_databases_output.database_list {
            // TODO: would be nice to skip this network call if we can tell that
            // the Glue database is not in the include
            let get_tables_output = glue
                .get_tables()
                .database_name(&db.name)
                .send()
                .await
                .context(GetTablesSnafu)?;

            let table_names = get_tables_output
                .table_list
                .unwrap_or_default()
                .into_iter()
                .filter(|t| {
                    is_supported(t) && is_included(catalog.include.as_ref(), &db.name, t.name())
                })
                .collect::<Vec<_>>();

            if !table_names.is_empty() {
                databases.insert(db.name, table_names);
            }
        }

        let inner = Arc::new(Inner {
            _glue: glue,
            databases,
            config,
        });

        Ok(Self { inner })
    }
}

fn is_supported(table: &Table) -> bool {
    if table
        .parameters
        .as_ref()
        .and_then(|params| params.get("table_type"))
        .is_some_and(|value| value.to_lowercase() == "iceberg")
    {
        return true; // Iceberg is supported
    }

    if table
        .storage_descriptor
        .as_ref()
        .and_then(|sd| sd.input_format.as_ref())
        .is_some_and(|input_format| {
            input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        })
    {
        return true; // Hive-style parquet is supported
    }

    false
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum TableType {
    HiveParquet,
    Iceberg,
    Unsupported,
}

impl TableType {
    fn from(table: &Table) -> TableType {
        if table
            .parameters
            .as_ref()
            .and_then(|params| params.get("table_type"))
            .is_some_and(|value| value.to_lowercase() == "iceberg")
        {
            return Self::Iceberg;
        }

        if table
            .storage_descriptor
            .as_ref()
            .and_then(|sd| sd.input_format.as_ref())
            .is_some_and(|input_format| {
                input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            })
        {
            return Self::HiveParquet;
        }

        Self::Unsupported
    }
}

fn is_included(include: Option<&GlobSet>, schema: &str, table: &str) -> bool {
    let schema_with_table = format!("{schema}.{table}");
    tracing::debug!("Checking if table {} should be included", schema_with_table);
    if let Some(include) = include {
        if !include.is_match(&schema_with_table) {
            tracing::debug!("Table {} is not included", schema_with_table);
            return false;
        }
    }
    true
}

impl GlueCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }
}

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("glue_aws_region")
        .description("The AWS region to use for Glue.")
        .secret(),
    ParameterSpec::component("glue_aws_access_key_id")
        .description("The AWS access key ID to use for Glue.")
        .secret(),
    ParameterSpec::component("glue_aws_secret_access_key")
        .description("The AWS secret access key to use for Glue.")
        .secret(),
    ParameterSpec::component("glue_aws_session_token")
        .description("The AWS session token to use for Glue.")
        .secret(),
];

#[async_trait]
impl CatalogConnector for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        Ok(Arc::new(
            GlueCatalogProvider::new(&self.params, catalog)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: "glue".to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        ))
    }
}

impl CatalogProvider for GlueCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.databases.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        if self.inner.databases.contains_key(name) {
            let schema_provider = GlueSchemaProvider {
                schema: name.to_string(),
                inner: Arc::clone(&self.inner),
            };
            Some(Arc::new(schema_provider))
        } else {
            None
        }
    }
}

#[async_trait]
impl RefreshableCatalogProvider for GlueCatalogProvider {
    async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // TODO
        Ok(())
    }
}

async fn load_config(params: &Parameters) -> SdkConfig {
    // Get and own all parameter values upfront
    let region = params
        .get("glue_aws_region")
        .expose()
        .ok()
        .unwrap_or("us-east-1")
        .to_string();

    let access_key_id = params
        .get("glue_aws_access_key_id")
        .expose()
        .ok()
        .map(ToString::to_string);

    let secret_access_key = params
        .get("glue_aws_secret_access_key")
        .expose()
        .ok()
        .map(ToString::to_string);

    let session_token = params
        .get("glue_aws_session_token")
        .expose()
        .ok()
        .map(ToString::to_string);

    match (access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let credentials = Credentials::new(
                access_key_id,
                secret_access_key,
                session_token,
                None,
                "GlueCatalogProvider",
            );

            aws_config::defaults(BehaviorVersion::v2025_01_17())
                .region(Region::new(region))
                .credentials_provider(credentials)
                .load()
                .await
        }
        _ => {
            // This will automatically load AWS credentials from the environment, via IAM roles if configured.
            aws_config::defaults(BehaviorVersion::v2025_01_17())
                .region(Region::new(region))
                .load()
                .await
        }
    }
}
