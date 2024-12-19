/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow_schema::DataType;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        expr::ScalarFunction, ColumnarValue, Extension, LogicalPlan, Projection, ScalarUDF,
        ScalarUDFImpl, Signature, TableScan,
    },
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
    prelude::Expr,
    sql::unparser::dialect::{Dialect, DuckDBDialect},
};
use datafusion_federation::{get_table_source, FederatedPlanNode};
use datafusion_federation_sql::SQLFederationProvider;

/// Implements `DataFusion` `AnalyzerRule` to replace `SpiceAI` internal UDFs with definitions that match
/// the signature of the target execution engine.
/// The rule is applied to `Projection` nodes of federated logical plans.
#[derive(Default, Debug)]
pub struct SpiceUDFsOverride {}

impl SpiceUDFsOverride {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SpiceUDFsOverride {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        if let LogicalPlan::Extension(extension_node) = plan {
            if let Some(node) = extension_node
                .node
                .as_any()
                .downcast_ref::<FederatedPlanNode>()
            {
                // extension plan node and logical plan are immutable references
                // so we need to clone the plan and reconstruct the extension node
                let updated_plan = node.plan().clone().transform(override_spice_udf)?;
                let planner = node.planner();

                let updated_extension_node = Extension {
                    node: Arc::new(FederatedPlanNode::new(updated_plan.data, planner)),
                };

                if updated_plan.transformed {
                    return Ok(Transformed::yes(LogicalPlan::Extension(updated_extension_node)));
                }
                return Ok(Transformed::no(LogicalPlan::Extension(updated_extension_node)));
            }
            return Ok(Transformed::no(LogicalPlan::Extension(extension_node)));
        }

        Ok(Transformed::no(plan))
    }

    fn name(&self) -> &str {
        "spiceai_udf_override_rule"
    }
}

/// Replaces `SpiceAI` UDFs with target execution engine-specific UDFs within the logical plan.
/// The transformation applies only to UDFs that are part of `Projection` nodes.
fn override_spice_udf(plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            // Note: while there is currently no in-place mutation API that uses `&mut TreeNode`,
            // the transforming APIs are efficient and optimized to avoid cloning.
            match override_spice_udf_in_exprs(&input, expr)? {
                (new_expr, true) => Ok(Transformed::yes(
                    Projection::try_new(new_expr, Arc::clone(&input))
                        .map(LogicalPlan::Projection)?,
                )),
                (new_expr, false) => Ok(Transformed::no(
                    Projection::try_new(new_expr, Arc::clone(&input))
                        .map(LogicalPlan::Projection)?,
                )),
            }
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn override_spice_udf_in_exprs(
    input: &LogicalPlan,
    exprs: Vec<Expr>,
) -> DataFusionResult<(Vec<Expr>, bool)> {
    let mut modified = false;
    let exprs: Vec<Expr> = exprs
        .into_iter()
        .map(|expr| {
            // recursive `transform` is required as UDF can be nested in the expression tree
            expr.transform(|expr| {
                if let Expr::ScalarFunction(scalar_fn) = expr {
                    match scalar_fn.name() {
                        "cosine_distance"
                            if is_duckdb_dialect(retrieve_dialect(input)?.as_ref()) =>
                        {
                            let rewritten_expr =
                                rewrite_cosine_distance_udf_duckdb(scalar_fn.func, scalar_fn.args)?;
                            modified = true;
                            return Ok(Transformed::yes(rewritten_expr));
                        }
                        _ => (),
                    }
                    return Ok(Transformed::no(Expr::ScalarFunction(scalar_fn)));
                }
                Ok(Transformed::no(expr))
            })
            .map(|x| x.data)
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    Ok((exprs, modified))
}

fn is_duckdb_dialect(dialect: Option<&Arc<dyn Dialect>>) -> bool {
    dialect.is_some_and(|dialect| dialect.as_any().is::<DuckDBDialect>())
}

/// Converts the `cosine_distance` UDF into `DuckDB` `array_cosine_distance` function:
/// `https://duckdb.org/docs/sql/functions/array.html#array_cosine_distancearray1-array2`
///
///  Replaces `make_array` function with `DuckDB` `array_value` function to convert list to `DuckDB` Array (`FixedSizeList`),
///  otherwise `DuckDB` will throw an error:
///
/// SQL Error: java.sql.SQLException: Binder Error: No function matches the given name and argument types `array_cosine_distance(FLOAT[384], DOUBLE[])`. You might need to add explicit type casts.
///  Candidate functions:
///  `array_cosine_distance(FLOAT[ANY], FLOAT[ANY])` -> FLOAT
///  `array_cosine_distance(DOUBLE[ANY], DOUBLE[ANY])` -> DOUBLE
///
fn rewrite_cosine_distance_udf_duckdb(
    func: Arc<ScalarUDF>,
    args: Vec<Expr>,
) -> DataFusionResult<Expr> {
    tracing::debug!("Rewriting `cosine_distance` UDF for DuckDB");
    let args: Vec<Expr> = args
        .into_iter()
        .map(|expr| {
            expr.transform(|expr| match expr {
                Expr::ScalarFunction(scalar_func)
                    if scalar_func.name().to_lowercase() == "make_array" =>
                {
                    // replace `make_array` with DuckDB `array_value`
                    let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
                        Arc::new(ScalarUDF::new_from_impl(RenameFunctionUDF::new(
                            "array_value",
                            scalar_func.func,
                        ))),
                        scalar_func.args,
                    ));
                    Ok(Transformed::yes(expr))
                }
                _ => Ok(Transformed::no(expr)),
            })
            .map(|x| x.data)
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    // Rename `cosine_distance` into `array_cosine_distance`
    let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(RenameFunctionUDF::new(
            "array_cosine_distance",
            func,
        ))),
        args,
    ));

    Ok(expr)
}

/// Recursively searches children of `LogicalPlan` to `TableScan` node and checks if the target execution engine is `DuckDB`
pub(crate) fn retrieve_dialect(plan: &LogicalPlan) -> DataFusionResult<Option<Arc<dyn Dialect>>> {
    let mut dialect: Option<Arc<dyn Dialect>> = None;

    plan.apply(|x| {
        if let Some(x) = retrieve_federated_node_dialect(x)? {
            dialect = Some(x);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(dialect)
}

fn retrieve_federated_node_dialect(
    plan: &LogicalPlan,
) -> DataFusionResult<Option<Arc<dyn Dialect>>> {
    match plan {
        LogicalPlan::TableScan(TableScan { ref source, .. }) => {
            let Some(federated_source) = get_table_source(source)? else {
                return Ok(None);
            };

            let provider = federated_source.federation_provider();

            let Some(sql_provider) = provider.as_any().downcast_ref::<SQLFederationProvider>()
            else {
                return Ok(None);
            };

            Ok(Some(sql_provider.dialect()))
        }
        _ => Ok(None),
    }
}

/// UDF function wrapper to provide different function name during unparsing
/// Implementation is requried as analyzer rules use them for coercion and type checking
#[derive(Debug)]
struct RenameFunctionUDF {
    name: String,
    inner: Arc<ScalarUDF>,
}

impl RenameFunctionUDF {
    fn new(name: &str, inner: Arc<ScalarUDF>) -> Self {
        Self {
            name: name.to_string(),
            inner,
        }
    }
}

impl ScalarUDFImpl for RenameFunctionUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn datafusion::common::ExprSchema,
        arg_types: &[DataType],
    ) -> DataFusionResult<DataType> {
        self.inner.return_type_from_exprs(args, schema, arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        // this method should not be called as `return_type_from_exprs` is implemented
        unreachable!("RenameFunctionUDF return_type should not be called")
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        // used by other rules to coerce types
        self.inner.coerce_types(arg_types)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        // UDF should be used for unparsing purpose only
        unreachable!("RenameFunctionUDF should not be invoked")
    }
}
