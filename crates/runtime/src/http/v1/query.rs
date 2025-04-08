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
use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use axum_extra::TypedHeader;
use datafusion::{common::ParamValues, scalar::ScalarValue};
use headers_accept::Accept;
use serde::Deserialize;
use serde_json::Value;

use crate::datafusion::DataFusion;

use super::{sql_to_http_response, ArrowFormat};

/// SQL Query
///
/// Execute a SQL query and return the results.
///
/// This endpoint allows users to execute SQL queries directly from an HTTP request. The SQL query is sent as plain text in the request body.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/sql",
    operation_id = "post_sql",
    tag = "SQL",
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'application/json' (default), 'text/csv' or 'text/plain'."),
    ),
    request_body(
        description = "SQL query to execute",
        content((
            String = "text/plain",
            example = "SELECT avg(total_amount), avg(tip_amount), count(1), passenger_count FROM my_table GROUP BY passenger_count ORDER BY passenger_count ASC LIMIT 3"
        ))
    ),
    responses(
        (status = 200, description = "SQL query executed successfully (JSON format)", content((
            Vec<serde_json::Value> = "application/json",
            example = json!([
                {
                    "AVG(my_table.tip_amount)": 3.072_259_971_396_793,
                    "AVG(my_table.total_amount)": 25.327_816_939_456_525,
                    "COUNT(Int64(1))": 31_465,
                    "passenger_count": 0
                },
                {
                    "AVG(my_table.tip_amount)": 3.371_262_288_468_005_7,
                    "AVG(my_table.total_amount)": 26.205_230_445_474_996,
                    "COUNT(Int64(1))": 2_188_739,
                    "passenger_count": 1
                },
                {
                    "AVG(my_table.tip_amount)": 3.717_130_211_329_085_4,
                    "AVG(my_table.total_amount)": 29.520_659_930_930_304,
                    "COUNT(Int64(1))": 405_103,
                    "passenger_count": 2
                }
            ])
        ),
        (
        String = "text/csv", example = r#""AVG(my_table.tip_amount)","AVG(my_table.total_amount)","COUNT(Int64(1))","passenger_count"
3.072259971396793,25.327816939456525,31465,0
3.3712622884680057,26.205230445474996,2188739,1
3.7171302113290854,29.520659930930304,405103,2"#
        ),
        (
            String = "text/plain",
            example = r#"
            +----------------------------+----------------------------+----------------+---------------------+
            | "AVG(my_table.tip_amount)"  | "AVG(my_table.total_amount)" | "COUNT(Int64(1))" | "passenger_count"   |
            +----------------------------+----------------------------+----------------+---------------------+
            | 3.072259971396793           | 25.327816939456525         | 31465          | 0                   |
            +----------------------------+----------------------------+----------------+---------------------+
            | 3.3712622884680057          | 26.205230445474996         | 2188739        | 1                   |
            +----------------------------+----------------------------+----------------+---------------------+
            | 3.7171302113290854          | 29.520659930930304         | 405103         | 2                   |
            +----------------------------+----------------------------+----------------+---------------------+"#
                )
        )),
        (status = 400, description = "Invalid SQL query or malformed input", content((
            String,
            example = "Error reading query: invalid UTF-8 sequence"
        ))),
        (status = 500, description = "Internal server error", content((
            String,
            example = "Unexpected internal server error occurred"
        )))
    )
))]
pub(crate) async fn sql(
    Extension(df): Extension<Arc<DataFusion>>,
    accept: Option<TypedHeader<Accept>>,
    body: Bytes,
) -> Response {
    let sql = match String::from_utf8(body.to_vec()) {
        Ok(query) => query,
        Err(e) => {
            tracing::debug!("Error reading query: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    sql_to_http_response(
        df,
        &sql,
        None,
        ArrowFormat::from_accept_header(accept.as_ref()),
    )
    .await
}

#[derive(Deserialize)]
pub(crate) struct PreparedStatement {
    sql: String,
    params: serde_json::Value,
}

// TODO: Add OpenAI docs
pub(crate) async fn prepared(
    Extension(df): Extension<Arc<DataFusion>>,
    accept: Option<TypedHeader<Accept>>,
    Json(PreparedStatement { sql, params }): Json<PreparedStatement>,
) -> Response {
    let params = match convert_json_to_param_values(params) {
        Ok(params) => params,
        Err(e) => {
            tracing::debug!("Error reading params: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    sql_to_http_response(
        df,
        &sql,
        Some(params),
        ArrowFormat::from_accept_header(accept.as_ref()),
    )
    .await
}

/// Converts a serde_json::Value into a datafusion::common::ParamValues.
///
/// # Arguments
/// * `json` - The JSON value to convert.
///
/// # Returns
/// * `Result<ParamValues, String>` - The converted ParamValues or an error message.
///
/// # Supported JSON Formats
/// - Array: Converted to positional parameters (ParamValues::Positional).
/// - Object: Converted to named parameters (ParamValues::Named).
/// - Primitive types (null, bool, number, string) are supported within arrays or objects.
///
/// # Errors
/// - Returns an error if the top-level JSON is not an array or object.
/// - Returns an error if any value cannot be converted to a ScalarValue.
fn convert_json_to_param_values(json: Value) -> Result<ParamValues, String> {
    match json {
        Value::Array(arr) => {
            let mut vec = Vec::with_capacity(arr.len());
            for (i, val) in arr.into_iter().enumerate() {
                let scalar = json_to_scalar(&val).map_err(|e| {
                    format!("failed to convert array element at index {}: {}", i, e)
                })?;
                vec.push(scalar);
            }
            Ok(ParamValues::List(vec))
        }
        Value::Object(obj) => {
            let mut map = HashMap::new();
            for (key, val) in obj {
                let scalar = json_to_scalar(&val)
                    .map_err(|e| format!("failed to convert value for key '{}': {}", key, e))?;
                map.insert(key, scalar);
            }
            Ok(ParamValues::Map(map))
        }
        _ => Err("params must be a JSON array or object".to_string()),
    }
}

/// Helper function to convert a single serde_json::Value to a ScalarValue.
///
/// # Arguments
/// * `json` - The JSON value to convert.
///
/// # Returns
/// * `Result<ScalarValue, String>` - The converted ScalarValue or an error message.
///
/// # Supported JSON Types
/// - Null -> ScalarValue::Utf8(None)
/// - Bool -> ScalarValue::Boolean(Some(bool))
/// - Number -> ScalarValue::Int64(Some(i64)) if integer, else ScalarValue::Float64(Some(f64))
/// - String -> ScalarValue::Utf8(Some(String))
/// - Arrays and objects return an error (handled at a higher level).
fn json_to_scalar(json: &Value) -> Result<ScalarValue, String> {
    match json {
        Value::Null => Ok(ScalarValue::Utf8(None)),
        Value::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ScalarValue::Int64(Some(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(ScalarValue::Float64(Some(f)))
            } else {
                Err("Unsupported JSON number format".to_string())
            }
        }
        Value::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        Value::Array(_) | Value::Object(_) => {
            Err("Nested arrays or objects are not supported as parameter values".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;
    use serde_json::json;
    use std::collections::HashMap;

    fn scalar_to_string(scalar: &ScalarValue) -> String {
        match scalar {
            ScalarValue::Int64(Some(i)) => format!("Int64({})", i),
            ScalarValue::Int64(None) => "Int64(None)".to_string(),
            ScalarValue::Float64(Some(f)) => format!("Float64({})", f),
            ScalarValue::Float64(None) => "Float64(None)".to_string(),
            ScalarValue::Utf8(Some(s)) => format!("Utf8(\"{}\")", s),
            ScalarValue::Utf8(None) => "Utf8(None)".to_string(),
            ScalarValue::Boolean(Some(b)) => format!("Boolean({})", b),
            ScalarValue::Boolean(None) => "Boolean(None)".to_string(),
            _ => unimplemented!(),
        }
    }

    fn param_values_to_string(params: ParamValues) -> String {
        match params {
            ParamValues::List(vec) => {
                let vals: Vec<String> = vec.iter().map(scalar_to_string).collect();
                format!("List([{}])", vals.join(", "))
            }
            ParamValues::Map(map) => {
                let mut items: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("\"{}\": {}", k, scalar_to_string(v)))
                    .collect();
                items.sort(); // Ensure consistent ordering
                format!("Map({{{}}})", items.join(", "))
            }
        }
    }

    fn assert_eq_param_values(a: ParamValues, b: ParamValues) {
        assert_eq!(param_values_to_string(a), param_values_to_string(b),);
    }

    #[test]
    fn test_json_array() {
        let json = json!([1, "hello", true, null]);
        let got = convert_json_to_param_values(json).unwrap();
        let want = ParamValues::List(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Utf8(None),
        ]);

        assert_eq_param_values(got, want);
    }

    #[test]
    fn test_json_object() {
        let json = json!({"x": 42, "y": "world", "z": false});
        let got = convert_json_to_param_values(json).unwrap();
        let mut want = HashMap::new();
        want.insert("x".to_string(), ScalarValue::Int64(Some(42)));
        want.insert(
            "y".to_string(),
            ScalarValue::Utf8(Some("world".to_string())),
        );
        want.insert("z".to_string(), ScalarValue::Boolean(Some(false)));
        assert_eq_param_values(got, ParamValues::Map(want));
    }

    #[test]
    fn test_invalid_top_level() {
        let json = json!(42);
        let result = convert_json_to_param_values(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_with_nested_array() {
        let json = json!([1, [2, 3]]);
        let result = convert_json_to_param_values(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_object_with_nested_object() {
        let json = json!({"a": 1, "b": {"c": 2}});
        let result = convert_json_to_param_values(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_array() {
        let json = json!([]);
        let result = convert_json_to_param_values(json).unwrap();
        assert_eq_param_values(result, ParamValues::List(vec![]));
    }

    #[test]
    fn test_empty_object() {
        let json = json!({});
        let result = convert_json_to_param_values(json).unwrap();
        assert_eq_param_values(result, ParamValues::Map(HashMap::new()));
    }

    #[test]
    fn test_array_with_nulls() {
        let json = json!([null, null]);
        let result = convert_json_to_param_values(json).unwrap();
        assert_eq_param_values(
            result,
            ParamValues::List(vec![ScalarValue::Utf8(None), ScalarValue::Utf8(None)]),
        );
    }

    #[test]
    fn test_object_with_nulls() {
        let json = json!({"a": null, "b": null});
        let result = convert_json_to_param_values(json).unwrap();
        let mut expected_map = HashMap::new();
        expected_map.insert("a".to_string(), ScalarValue::Utf8(None));
        expected_map.insert("b".to_string(), ScalarValue::Utf8(None));
        assert_eq_param_values(result, ParamValues::Map(expected_map));
    }

    #[test]
    fn test_array_with_floats() {
        let json = json!([1.5, 2.0]);
        let result = convert_json_to_param_values(json).unwrap();
        assert_eq_param_values(
            result,
            ParamValues::List(vec![
                ScalarValue::Float64(Some(1.5)),
                ScalarValue::Float64(Some(2.0)),
            ]),
        );
    }

    #[test]
    fn test_object_with_floats() {
        let json = json!({"pi": 3.14, "e": 2.718});
        let result = convert_json_to_param_values(json).unwrap();
        let mut expected_map = HashMap::new();
        expected_map.insert("pi".to_string(), ScalarValue::Float64(Some(3.14)));
        expected_map.insert("e".to_string(), ScalarValue::Float64(Some(2.718)));
        assert_eq_param_values(result, ParamValues::Map(expected_map));
    }

    #[test]
    fn test_array_with_strings_and_bools() {
        let json = json!(["test", true, false]);
        let result = convert_json_to_param_values(json).unwrap();
        assert_eq_param_values(
            result,
            ParamValues::List(vec![
                ScalarValue::Utf8(Some("test".to_string())),
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
            ]),
        );
    }

    #[test]
    fn test_object_with_strings_and_bools() {
        let json = json!({"name": "Alice", "is_active": true});
        let result = convert_json_to_param_values(json).unwrap();
        let mut expected_map = HashMap::new();
        expected_map.insert(
            "name".to_string(),
            ScalarValue::Utf8(Some("Alice".to_string())),
        );
        expected_map.insert("is_active".to_string(), ScalarValue::Boolean(Some(true)));
        assert_eq_param_values(result, ParamValues::Map(expected_map));
    }

    #[test]
    fn test_error_with_specific_index() {
        let json = json!([1, "two", [3]]);
        let result = convert_json_to_param_values(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_error_with_specific_key() {
        let json = json!({"a": 1, "b": "two", "c": {"d": 3}});
        let result = convert_json_to_param_values(json);
        assert!(result.is_err());
    }
}
