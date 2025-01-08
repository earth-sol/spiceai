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

use app::AppBuilder;
use runtime::Runtime;

use crate::results::BenchmarkResultsBuilder;
use spicepod::component::{dataset::Dataset, params::Params};
use test_framework::queries::{get_tpcds_test_queries, get_tpch_test_queries};

pub(crate) async fn run(
    rt: &mut Runtime,
    benchmark_results: &mut BenchmarkResultsBuilder,
    bench_name: &str,
) -> Result<(), String> {
    let test_queries = match bench_name {
        "tpch" => get_tpch_test_queries(None),
        "tpcds" => get_tpcds_test_queries(None),
        _ => return Err(format!("Invalid benchmark to run {bench_name}")),
    };

    let mut errors = Vec::new();

    for (query_name, query) in test_queries {
        let verify_query_results =
            query_name.starts_with("tpch_q") || query_name.starts_with("tpcds_q");
        if let Err(e) = super::run_query_and_record_result(
            rt,
            benchmark_results,
            "databricks_delta",
            query_name,
            query,
            verify_query_results,
        )
        .await
        {
            errors.push(format!("Query {query_name} failed with error: {e}"));
        };
    }

    if !errors.is_empty() {
        tracing::error!("There are failed queries:\n{}", errors.join("\n"));
    }

    Ok(())
}

pub fn build_app(app_builder: AppBuilder, bench_name: &str) -> Result<AppBuilder, String> {
    match bench_name {
        "tpch" => Ok(app_builder
            .with_dataset(make_dataset("spiceai_sandbox.tpch.customer", "customer"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.lineitem", "lineitem"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.part", "part"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.partsupp", "partsupp"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.orders", "orders"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.nation", "nation"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.region", "region"))
            .with_dataset(make_dataset("spiceai_sandbox.tpch.supplier", "supplier"))),
        "tpcds" => Ok(app_builder
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.call_center",
                "call_center",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.catalog_page",
                "catalog_page",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.catalog_returns",
                "catalog_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.catalog_sales",
                "catalog_sales",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.customer",
                "customer",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.customer_address",
                "customer_address",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.customer_demographics",
                "customer_demographics",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.date_dim",
                "date_dim",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.household_demographics",
                "household_demographics",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.income_band",
                "income_band",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.inventory",
                "inventory",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds_sf5.item", "item"))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.promotion",
                "promotion",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds_sf5.reason", "reason"))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.ship_mode",
                "ship_mode",
            ))
            .with_dataset(make_dataset("spiceai_sandbox.tpcds_sf5.store", "store"))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.store_returns",
                "store_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.store_sales",
                "store_sales",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.time_dim",
                "time_dim",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.warehouse",
                "warehouse",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.web_page",
                "web_page",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.web_returns",
                "web_returns",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.web_sales",
                "web_sales",
            ))
            .with_dataset(make_dataset(
                "spiceai_sandbox.tpcds_sf5.web_site",
                "web_site",
            ))),
        _ => Err("Only tpcds or tpch benchmark suites are supported".to_string()),
    }
}

fn make_dataset(path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("databricks:{path}"), name.to_string());
    dataset.params = Some(get_params());
    dataset
}

fn get_params() -> Params {
    Params::from_string_map(
        vec![
            (
                "databricks_endpoint".to_string(),
                "${ env:DATABRICKS_HOST }".to_string(),
            ),
            (
                "databricks_token".to_string(),
                "${ env:DATABRICKS_TOKEN }".to_string(),
            ),
            (
                "databricks_aws_secret_access_key".to_string(),
                "${ env:AWS_DATABRICKS_DELTA_SECRET_ACCESS_KEY }".to_string(),
            ),
            (
                "databricks_aws_access_key_id".to_string(),
                "${ env:AWS_DATABRICKS_DELTA_ACCESS_KEY_ID }".to_string(),
            ),
            ("client_timeout".to_string(), "120s".to_string()),
            ("mode".to_string(), "delta_lake".to_string()),
        ]
        .into_iter()
        .collect(),
    )
}
