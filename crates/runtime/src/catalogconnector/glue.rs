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
use crate::{Runtime, component::catalog::Catalog, dataconnector::ConnectorParams};
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_glue::Client;
use aws_sdk_sts::config::Credentials;
use data_components::RefreshableCatalogProvider;
use datafusion::catalog::CatalogProvider;
use snafu::prelude::*;
use std::{any::Any, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct GlueCatalog {
    params: Parameters,
}

#[derive(Debug)]
pub struct GlueCatalogProvider {
    glue: Client,
    schema_names: Vec<String>,
}

impl GlueCatalogProvider {
    pub async fn new(params: &Parameters) -> Self {
        let config = load_config(params).await;
        let glue = Client::new(dbg!(&config));

        let list_schemas_output = glue.list_schemas().send().await.unwrap();
        let schema_names = dbg!(list_schemas_output)
            .schemas()
            .iter()
            .filter_map(|item| item.schema_name.clone())
            .collect();

        Self { glue, schema_names }
    }
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
        Ok(Arc::new(GlueCatalogProvider::new(&self.params).await))
    }
}

impl CatalogProvider for GlueCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schema_names.clone()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::SchemaProvider>> {
        None
    }
}

#[async_trait]
impl RefreshableCatalogProvider for GlueCatalogProvider {
    async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

async fn load_config(params: &Parameters) -> SdkConfig {
    // Get and own all parameter values upfront
    let region = params
        .get("glue_aws_region")
        .expose()
        .ok()
        .unwrap()
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

    let config = match (access_key_id, secret_access_key) {
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
    };

    config
}
