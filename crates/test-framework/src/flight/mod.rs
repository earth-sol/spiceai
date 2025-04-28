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

use std::sync::Arc;

use anyhow::Result;
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{Field, FieldRef, Fields, Schema},
    record_batch::RecordBatch,
};
use arrow_flight::sql::client::FlightSqlServiceClient;
use flight_client::FlightClient;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::Channel;

/// Query a flight client and return the result as a vector of record batches
///
/// # Errors
///
/// - If the flight client fails to query
pub async fn query_to_batches(client: &FlightClient, sql: &str) -> Result<Vec<RecordBatch>> {
    let mut stream = client.query(sql).await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

pub async fn put_batches(
    client: &mut FlightClient,
    dataset_path: &str,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    Ok(client.publish(dataset_path, batches).await?)
}

/// Query a prepared statement against a FlightSQL compatible server.
///
/// To construct `parameters` as a [`RecordBatch`], see [`create_param_batch`].
async fn execute_prepared_statement(
    client: &mut FlightSqlServiceClient<Channel>,
    query: &str,
    parameters: RecordBatch,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut prepared_stmt = client.prepare(query.to_string(), None).await?;

    prepared_stmt.set_parameters(parameters)?;

    let flight_info = prepared_stmt.execute().await?;

    let mut results = Vec::new();

    for endpoint in &flight_info.endpoint {
        let ticket = endpoint
            .ticket
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No ticket in endpoint"))?;

        let stream = client.do_get(ticket.clone()).await?;
        let mut batch_results: Vec<RecordBatch> = stream.try_collect().await?;

        results.append(&mut batch_results);
    }

    Ok(results)
}

pub struct PreparedStatementParamColumn<'a> {
    name: &'a str,
    dtype: arrow::datatypes::DataType,
    nullable: bool,
    array: ArrayRef,
}

impl<'a> PreparedStatementParamColumn<'a> {
    pub fn new(
        name: &'a str,
        dtype: arrow::datatypes::DataType,
        nullable: bool,
        array: ArrayRef,
    ) -> Self {
        Self {
            name,
            dtype,
            nullable,
            array,
        }
    }
}

/// # Usage
///
/// ```rust
/// create_param_batch(vec![
///   PreparedStatementParamColumn::new(
///     "$1",
///     arrow::datatypes::DataType::Int64,
///     false,
///     Arc::new(Int64Array::from(vec![41])) as Arc<dyn arrow::array::Array>
///   ),
///   PreparedStatementParamColumn::new(
///     "$2",
///     arrow::datatypes::DataType::Utf8,
///     true,
///     Arc::new(StringArray::from(vec![Some(41), 42])) as Arc<dyn arrow::array::Array>
///   )
/// ])?;
/// ```
fn create_param_batch(
    params: Vec<PreparedStatementParamColumn>,
) -> Result<RecordBatch, anyhow::Error> {
    let (fields, columns): (Vec<FieldRef>, Vec<ArrayRef>) = params
        .iter()
        .map(|col| {
            let PreparedStatementParamColumn {
                name,
                dtype,
                nullable,
                array,
            } = col.clone();
            (
                Arc::new(Field::new(name.clone(), dtype.clone(), *nullable)),
                Arc::clone(&array),
            )
        })
        .unzip();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(Into::into)
}
