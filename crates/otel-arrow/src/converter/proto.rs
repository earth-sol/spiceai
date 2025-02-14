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

use std::{
    borrow::Cow,
    time::{Duration, SystemTime},
};

use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::{
    self,
    common::v1::any_value,
    metrics::v1::{
        exponential_histogram_data_point, metric::Data, number_data_point, NumberDataPoint,
        ResourceMetrics as ProtoResourceMetrics,
    },
};
use opentelemetry_sdk::{
    metrics::{
        data::{
            Aggregation, DataPoint, ExponentialBucket, ExponentialHistogram,
            ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, Metric,
            ResourceMetrics, ScopeMetrics, Sum,
        },
        Temporality,
    },
    Resource,
};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported bytes value in attributes"))]
    UnsupportedBytesValueAttributes,

    #[snafu(display("Unsupported kvlist value in attributes"))]
    UnsupportedKvlistValueAttributes,

    #[snafu(display("Unsupported array value in attributes"))]
    UnsupportedArrayValueAttributes,

    #[snafu(display("Unsupported summary data"))]
    UnsupportedSummaryData,

    #[snafu(display("Missing value"))]
    MissingValue,

    #[snafu(display("Unsupported temporality"))]
    UnsupportedTemporality,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Convert a protobuf `ResourceMetrics` to the SDK `ResourceMetrics` type
///
/// # Errors
///
/// Returns an error if:
/// - The attributes in the resource cannot be converted
/// - The scope metrics contain unsupported data types
/// - Required values are missing
/// - Unsupported temporality is specified
pub fn proto_to_sdk(proto: ProtoResourceMetrics) -> Result<ResourceMetrics> {
    let mut resource_kvs = vec![];
    if let Some(resource) = proto.resource {
        resource_kvs = convert_proto_attributes(resource.attributes)?;
    }
    let resource = Resource::from_schema_url(resource_kvs, proto.schema_url);
    let mut scope_metrics = vec![];
    for scope_metric in proto.scope_metrics {
        let mut scope_builder = InstrumentationScope::builder(
            scope_metric
                .scope
                .as_ref()
                .map(|s| s.name.to_string())
                .unwrap_or_default(),
        )
        .with_schema_url(scope_metric.schema_url);
        if let Some(scope) = scope_metric.scope {
            scope_builder = scope_builder
                .with_version(scope.version)
                .with_attributes(convert_proto_attributes(scope.attributes)?);
        }
        let scope = scope_builder.build();
        let mut metrics = vec![];
        for metric in scope_metric.metrics {
            let name = metric.name;
            let description = metric.description;
            let unit = metric.unit;
            if let Some(data) = metric.data {
                match data {
                    Data::Gauge(gauge) => {
                        let data = convert_gauge_data_points(gauge.data_points)?;
                        metrics.push(Metric {
                            name: Cow::Owned(name),
                            description: Cow::Owned(description),
                            unit: Cow::Owned(unit),
                            data,
                        });
                    }
                    Data::Sum(sum) => {
                        let data = convert_sum_data_points(
                            sum.data_points,
                            to_temporality(sum.aggregation_temporality)?,
                            sum.is_monotonic,
                        )?;
                        metrics.push(Metric {
                            name: Cow::Owned(name),
                            description: Cow::Owned(description),
                            unit: Cow::Owned(unit),
                            data,
                        });
                    }
                    Data::Histogram(histogram) => {
                        let data = convert_histogram(histogram)?;
                        metrics.push(Metric {
                            name: Cow::Owned(name),
                            description: Cow::Owned(description),
                            unit: Cow::Owned(unit),
                            data: Box::new(data),
                        });
                    }
                    Data::ExponentialHistogram(exponential_histogram) => {
                        let data =
                            convert_exponential_histogram_data_points(exponential_histogram)?;
                        metrics.push(Metric {
                            name: Cow::Owned(name),
                            description: Cow::Owned(description),
                            unit: Cow::Owned(unit),
                            data: Box::new(data),
                        });
                    }
                    Data::Summary(_) => return Err(Error::UnsupportedSummaryData),
                }
            }
        }
        scope_metrics.push(ScopeMetrics { scope, metrics });
    }
    Ok(ResourceMetrics {
        resource,
        scope_metrics,
    })
}

fn convert_proto_attributes(
    attributes: impl IntoIterator<Item = opentelemetry_proto::tonic::common::v1::KeyValue>,
) -> Result<Vec<KeyValue>> {
    let mut kvs = vec![];
    for kv in attributes {
        if let Some(value) = kv.value.and_then(|v| v.value) {
            match value {
                any_value::Value::StringValue(s) => kvs.push(KeyValue::new(kv.key, s)),
                any_value::Value::IntValue(i) => kvs.push(KeyValue::new(kv.key, i)),
                any_value::Value::DoubleValue(d) => kvs.push(KeyValue::new(kv.key, d)),
                any_value::Value::BoolValue(b) => kvs.push(KeyValue::new(kv.key, b)),
                any_value::Value::ArrayValue(_) => {
                    return Err(Error::UnsupportedArrayValueAttributes);
                }
                any_value::Value::KvlistValue(_) => {
                    return Err(Error::UnsupportedKvlistValueAttributes);
                }
                any_value::Value::BytesValue(_) => {
                    return Err(Error::UnsupportedBytesValueAttributes);
                }
            }
        }
    }
    Ok(kvs)
}

#[allow(clippy::cast_precision_loss)]
fn convert_number_data_point_f64(points: Vec<NumberDataPoint>) -> Result<Vec<DataPoint<f64>>> {
    points
        .into_iter()
        .map(|dp| match dp.value {
            Some(number_data_point::Value::AsDouble(d)) => Ok(DataPoint {
                attributes: convert_proto_attributes(dp.attributes)?,
                start_time: to_system_time(dp.start_time_unix_nano),
                time: to_system_time(dp.time_unix_nano),
                value: d,
                exemplars: vec![],
            }),
            Some(number_data_point::Value::AsInt(i)) => Ok(DataPoint {
                attributes: convert_proto_attributes(dp.attributes)?,
                start_time: to_system_time(dp.start_time_unix_nano),
                time: to_system_time(dp.time_unix_nano),
                value: i as f64,
                exemplars: vec![],
            }),
            None => Err(Error::MissingValue),
        })
        .collect()
}

#[allow(clippy::cast_possible_truncation)]
fn convert_number_data_point_i64(points: Vec<NumberDataPoint>) -> Result<Vec<DataPoint<i64>>> {
    points
        .into_iter()
        .map(|dp| match dp.value {
            Some(number_data_point::Value::AsDouble(d)) => Ok(DataPoint {
                attributes: convert_proto_attributes(dp.attributes)?,
                start_time: to_system_time(dp.start_time_unix_nano),
                time: to_system_time(dp.time_unix_nano),
                value: d as i64,
                exemplars: vec![],
            }),
            Some(number_data_point::Value::AsInt(i)) => Ok(DataPoint {
                attributes: convert_proto_attributes(dp.attributes)?,
                start_time: to_system_time(dp.start_time_unix_nano),
                time: to_system_time(dp.time_unix_nano),
                value: i,
                exemplars: vec![],
            }),
            None => Err(Error::MissingValue),
        })
        .collect()
}

fn convert_gauge_data_points(points: Vec<NumberDataPoint>) -> Result<Box<dyn Aggregation>> {
    if let Some(first_point) = points.first() {
        match first_point.value {
            Some(number_data_point::Value::AsDouble(_)) => {
                // Convert all points to f64
                Ok(Box::new(Gauge {
                    data_points: convert_number_data_point_f64(points)?,
                }))
            }
            Some(number_data_point::Value::AsInt(_)) => {
                // Convert all points to i64
                Ok(Box::new(Gauge {
                    data_points: convert_number_data_point_i64(points)?,
                }))
            }
            None => Err(Error::MissingValue),
        }
    } else {
        Ok(Box::new(Gauge::<i64> {
            data_points: vec![],
        }))
    }
}

fn convert_sum_data_points(
    points: Vec<NumberDataPoint>,
    temporality: Temporality,
    is_monotonic: bool,
) -> Result<Box<dyn Aggregation>> {
    if let Some(first_point) = points.first() {
        match first_point.value {
            Some(number_data_point::Value::AsDouble(_)) => {
                // Convert all points to f64
                Ok(Box::new(Sum {
                    data_points: convert_number_data_point_f64(points)?,
                    temporality,
                    is_monotonic,
                }))
            }
            Some(number_data_point::Value::AsInt(_)) => {
                // Convert all points to i64
                Ok(Box::new(Sum {
                    data_points: convert_number_data_point_i64(points)?,
                    temporality,
                    is_monotonic,
                }))
            }
            None => Err(Error::MissingValue),
        }
    } else {
        Ok(Box::new(Sum::<i64> {
            data_points: vec![],
            temporality,
            is_monotonic,
        }))
    }
}

fn convert_histogram(histogram: tonic::metrics::v1::Histogram) -> Result<Histogram<f64>> {
    let mut data_points = vec![];
    for point in histogram.data_points {
        let data_point = HistogramDataPoint {
            attributes: convert_proto_attributes(point.attributes)?,
            start_time: must_to_system_time(point.start_time_unix_nano),
            time: must_to_system_time(point.time_unix_nano),
            count: point.count,
            bounds: point.explicit_bounds,
            bucket_counts: point.bucket_counts,
            min: point.min,
            max: point.max,
            sum: point.sum.unwrap_or_default(),
            exemplars: vec![],
        };
        data_points.push(data_point);
    }
    Ok(Histogram {
        data_points,
        temporality: to_temporality(histogram.aggregation_temporality)?,
    })
}

#[allow(clippy::cast_possible_truncation)]
fn convert_exponential_histogram_data_points(
    exponential_histogram: tonic::metrics::v1::ExponentialHistogram,
) -> Result<ExponentialHistogram<f64>> {
    let mut data_points = vec![];
    for point in exponential_histogram.data_points {
        let data_point = ExponentialHistogramDataPoint {
            attributes: convert_proto_attributes(point.attributes)?,
            start_time: must_to_system_time(point.start_time_unix_nano),
            time: must_to_system_time(point.time_unix_nano),
            count: point.count as usize,
            min: point.min,
            max: point.max,
            sum: point.sum.unwrap_or_default(),
            scale: point.scale as i8,
            zero_count: point.zero_count,
            positive_bucket: proto_bucket_to_exponential_bucket(point.positive),
            negative_bucket: proto_bucket_to_exponential_bucket(point.negative),
            zero_threshold: point.zero_threshold,
            exemplars: vec![],
        };
        data_points.push(data_point);
    }
    Ok(ExponentialHistogram {
        data_points,
        temporality: to_temporality(exponential_histogram.aggregation_temporality)?,
    })
}

fn to_system_time(unix_nano: u64) -> Option<SystemTime> {
    if unix_nano == 0 {
        None
    } else {
        Some(SystemTime::UNIX_EPOCH + Duration::from_nanos(unix_nano))
    }
}

fn must_to_system_time(unix_nano: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_nanos(unix_nano)
}

fn to_temporality(value: i32) -> Result<Temporality> {
    match value {
        1 => Ok(Temporality::Delta),
        2 => Ok(Temporality::Cumulative),
        _ => Err(Error::UnsupportedTemporality),
    }
}

fn proto_bucket_to_exponential_bucket(
    bucket: Option<exponential_histogram_data_point::Buckets>,
) -> ExponentialBucket {
    match bucket {
        Some(bucket) => ExponentialBucket {
            offset: bucket.offset,
            counts: bucket.bucket_counts,
        },
        None => ExponentialBucket {
            offset: 0,
            counts: vec![],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue as ProtoKeyValue},
        metrics::v1::{
            AggregationTemporality, Gauge as ProtoGauge, Histogram as ProtoHistogram,
            HistogramDataPoint as ProtoHistogramDataPoint, Metric as ProtoMetric,
            NumberDataPoint as ProtoNumberDataPoint, ScopeMetrics as ProtoScopeMetrics,
            Sum as ProtoSum,
        },
    };

    const EPSILON: f64 = 1e-10;

    fn assert_float_eq(a: f64, b: f64) {
        assert!(
            (a - b).abs() < EPSILON,
            "Expected {a} to be approximately equal to {b}",
        );
    }

    fn create_test_number_datapoint(value: number_data_point::Value) -> ProtoNumberDataPoint {
        ProtoNumberDataPoint {
            attributes: vec![ProtoKeyValue {
                key: "test_key".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("test_value".to_string())),
                }),
            }],
            start_time_unix_nano: 1_000_000_000,
            time_unix_nano: 2_000_000_000,
            value: Some(value),
            ..Default::default()
        }
    }

    #[test]
    fn test_gauge_conversion() -> Result<()> {
        let proto_gauge = ProtoGauge {
            data_points: vec![
                create_test_number_datapoint(number_data_point::Value::AsDouble(42.5)),
                create_test_number_datapoint(number_data_point::Value::AsDouble(43.5)),
            ],
        };

        let gauge = convert_gauge_data_points(proto_gauge.data_points)?;
        let gauge = gauge
            .as_any()
            .downcast_ref::<Gauge<f64>>()
            .expect("Should be f64 gauge");

        assert_eq!(gauge.data_points.len(), 2);
        assert_float_eq(gauge.data_points[0].value, 42.5);
        assert_float_eq(gauge.data_points[1].value, 43.5);

        Ok(())
    }

    #[test]
    fn test_sum_conversion() -> Result<()> {
        let proto_sum = ProtoSum {
            data_points: vec![create_test_number_datapoint(
                number_data_point::Value::AsInt(42),
            )],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        };

        let sum = convert_sum_data_points(
            proto_sum.data_points,
            Temporality::Cumulative,
            proto_sum.is_monotonic,
        )?;
        let sum = sum
            .as_any()
            .downcast_ref::<Sum<i64>>()
            .expect("Should be i64 sum");

        assert_eq!(sum.data_points.len(), 1);
        assert_eq!(sum.data_points[0].value, 42);
        assert!(sum.is_monotonic);
        assert_eq!(sum.temporality, Temporality::Cumulative);

        Ok(())
    }

    #[test]
    fn test_histogram_conversion() -> Result<()> {
        let proto_histogram = ProtoHistogram {
            data_points: vec![ProtoHistogramDataPoint {
                attributes: vec![ProtoKeyValue {
                    key: "test_key".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test_value".to_string())),
                    }),
                }],
                start_time_unix_nano: 1_000_000_000,
                time_unix_nano: 2_000_000_000,
                count: 10,
                sum: Some(100.0),
                bucket_counts: vec![2, 3, 5],
                explicit_bounds: vec![10.0, 20.0],
                min: Some(5.0),
                max: Some(25.0),
                ..Default::default()
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        };

        let histogram = convert_histogram(proto_histogram)?;

        assert_eq!(histogram.data_points.len(), 1);
        let point = &histogram.data_points[0];
        assert_eq!(point.count, 10);
        assert_float_eq(point.sum, 100.0);
        assert_eq!(point.bucket_counts, vec![2, 3, 5]);
        assert!(point
            .bounds
            .iter()
            .zip([10.0, 20.0].iter())
            .all(|(a, b)| (a - b).abs() < EPSILON));
        assert!(point.min.is_some_and(|v| (v - 5.0).abs() < EPSILON));
        assert!(point.max.is_some_and(|v| (v - 25.0).abs() < EPSILON));

        Ok(())
    }

    #[test]
    fn test_resource_metrics_conversion() -> Result<()> {
        let proto_resource_metrics = ProtoResourceMetrics {
            resource: None,
            schema_url: "test_schema".to_string(),
            scope_metrics: vec![ProtoScopeMetrics {
                scope: None,
                schema_url: "test_scope_schema".to_string(),
                metrics: vec![ProtoMetric {
                    name: "test_metric".to_string(),
                    description: "test description".to_string(),
                    unit: "test_unit".to_string(),
                    data: Some(Data::Gauge(ProtoGauge {
                        data_points: vec![create_test_number_datapoint(
                            number_data_point::Value::AsDouble(42.5),
                        )],
                    })),
                    metadata: vec![],
                }],
            }],
        };

        let resource_metrics = proto_to_sdk(proto_resource_metrics)?;

        assert_eq!(resource_metrics.scope_metrics.len(), 1);
        let scope_metrics = &resource_metrics.scope_metrics[0];
        assert_eq!(scope_metrics.metrics.len(), 1);
        let metric = &scope_metrics.metrics[0];
        assert_eq!(metric.name, "test_metric");
        assert_eq!(metric.description, "test description");
        assert_eq!(metric.unit, "test_unit");

        Ok(())
    }

    #[test]
    fn test_attribute_conversion() -> Result<()> {
        let proto_attributes = vec![
            ProtoKeyValue {
                key: "string_key".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("string_value".to_string())),
                }),
            },
            ProtoKeyValue {
                key: "int_key".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(42)),
                }),
            },
            ProtoKeyValue {
                key: "double_key".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::DoubleValue(42.5)),
                }),
            },
            ProtoKeyValue {
                key: "bool_key".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                }),
            },
        ];

        let kvs = convert_proto_attributes(proto_attributes)?;

        assert_eq!(kvs.len(), 4);
        assert_eq!(kvs[0].key.as_str(), "string_key");
        assert_eq!(kvs[1].key.as_str(), "int_key");
        assert_eq!(kvs[2].key.as_str(), "double_key");
        assert_eq!(kvs[3].key.as_str(), "bool_key");

        Ok(())
    }

    #[test]
    fn test_error_cases() {
        // Test missing value
        let result = convert_number_data_point_f64(vec![ProtoNumberDataPoint {
            value: None,
            ..Default::default()
        }]);
        assert!(matches!(result, Err(Error::MissingValue)));

        // Test unsupported temporality
        let result = to_temporality(0);
        assert!(matches!(result, Err(Error::UnsupportedTemporality)));

        // Test unsupported attribute types
        let proto_attributes = vec![ProtoKeyValue {
            key: "bytes_key".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::BytesValue(vec![1, 2, 3])),
            }),
        }];
        let result = convert_proto_attributes(proto_attributes);
        assert!(matches!(
            result,
            Err(Error::UnsupportedBytesValueAttributes)
        ));
    }
}
