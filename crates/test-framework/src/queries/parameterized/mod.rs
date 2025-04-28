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

use super::{ParameterValue, Query};

/// Defines parameters for TPC-H queries. Values are extracted from the original TPC-H queries,
/// with their values replaced with ? parameters in the `/parameterized/` TPC-H files.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn add_tpch_parameters(queries: Vec<Query>) -> Vec<Query> {
    queries
        .into_iter()
        .map(|q| {
            let mut q = q;
            match q.name.as_ref() {
                "q1" => q.parameters = Some(vec![ParameterValue::String("1998-09-02".into())]),
                "q2" => {
                    q.parameters = Some(vec![
                        ParameterValue::Number(15),
                        ParameterValue::String("%BRASS".into()),
                        ParameterValue::String("EUROPE".into()),
                        ParameterValue::String("EUROPE".into()),
                    ]);
                }
                "q3" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("BUILDING".into()),
                        ParameterValue::String("1995-03-15".into()),
                        ParameterValue::String("1995-03-15".into()),
                    ]);
                }
                "q4" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("1993-07-01".into()),
                        ParameterValue::String("1993-07-01".into()),
                        ParameterValue::String("3".into()),
                    ]);
                }
                "q5" => {
                    q.parameters = Some(vec![ParameterValue::String("ASIA".into())]);
                }
                "q6" => {
                    q.parameters = Some(vec![
                        ParameterValue::Float(0.06),
                        ParameterValue::Float(0.01),
                        ParameterValue::Float(0.06),
                        ParameterValue::Float(0.01),
                        ParameterValue::Number(24),
                    ]);
                }
                "q7" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("FRANCE".into()),
                        ParameterValue::String("GERMANY".into()),
                        ParameterValue::String("GERMANY".into()),
                        ParameterValue::String("FRANCE".into()),
                        ParameterValue::String("1995-01-01".into()),
                        ParameterValue::String("1996-12-31".into()),
                    ]);
                }
                "q8" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("BRAZIL".into()),
                        ParameterValue::String("AMERICA".into()),
                        ParameterValue::String("1995-01-01".into()),
                        ParameterValue::String("1996-12-31".into()),
                        ParameterValue::String("ECONOMY ANODIZED STEEL".into()),
                    ]);
                }
                "q9" => {
                    q.parameters = Some(vec![ParameterValue::String("%green%".into())]);
                }
                "q10" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("1993-10-01".into()),
                        ParameterValue::String("1994-01-01".into()),
                        ParameterValue::String("R".into()),
                    ]);
                }
                "q11" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("GERMANY".into()),
                        ParameterValue::String("GERMANY".into()),
                    ]);
                }
                "q12" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("1-URGENT".into()),
                        ParameterValue::String("2-HIGH".into()),
                        ParameterValue::String("1-URGENT".into()),
                        ParameterValue::String("2-HIGH".into()),
                        ParameterValue::String("MAIL".into()),
                        ParameterValue::String("SHIP".into()),
                        ParameterValue::String("1994-01-01".into()),
                        ParameterValue::String("1995-01-01".into()),
                    ]);
                }
                "q13" => {
                    q.parameters = Some(vec![ParameterValue::String("%special%requests%".into())]);
                }
                "q14" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("PROMO%".into()),
                        ParameterValue::Number(1),
                        ParameterValue::Number(0),
                        ParameterValue::Number(1),
                        ParameterValue::String("1995-09-01".into()),
                        ParameterValue::String("1995-10-01".into()),
                    ]);
                }
                "q16" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("Brand#45".into()),
                        ParameterValue::String("MEDIUM POLISHED%".into()),
                        ParameterValue::Number(49),
                        ParameterValue::Number(14),
                        ParameterValue::Number(23),
                        ParameterValue::Number(45),
                        ParameterValue::Number(19),
                        ParameterValue::Number(3),
                        ParameterValue::Number(36),
                        ParameterValue::Number(9),
                        ParameterValue::String("%Customer%Complaints%".into()),
                    ]);
                }
                "q17" => {
                    q.parameters = Some(vec![
                        ParameterValue::Float(7.0),
                        ParameterValue::String("Brand#23".into()),
                        ParameterValue::String("MED BOX".into()),
                        ParameterValue::Float(0.2),
                    ]);
                }
                "q18" => {
                    q.parameters = Some(vec![ParameterValue::Number(300)]);
                }
                "q19" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("Brand#12".into()),
                        ParameterValue::String("SM CASE".into()),
                        ParameterValue::String("SM BOX".into()),
                        ParameterValue::String("SM PACK".into()),
                        ParameterValue::String("SM PKG".into()),
                        ParameterValue::Number(1),
                        ParameterValue::Number(1),
                        ParameterValue::Number(10),
                        ParameterValue::Number(1),
                        ParameterValue::Number(5),
                        ParameterValue::String("AIR".into()),
                        ParameterValue::String("AIR REG".into()),
                        ParameterValue::String("DELIVER IN PERSON".into()),
                        ParameterValue::String("Brand#23".into()),
                        ParameterValue::String("MED BAG".into()),
                        ParameterValue::String("MED BOX".into()),
                        ParameterValue::String("MED PKG".into()),
                        ParameterValue::String("MED PACK".into()),
                        ParameterValue::Number(10),
                        ParameterValue::Number(10),
                        ParameterValue::Number(10),
                        ParameterValue::Number(1),
                        ParameterValue::Number(10),
                        ParameterValue::String("AIR".into()),
                        ParameterValue::String("AIR REG".into()),
                        ParameterValue::String("DELIVER IN PERSON".into()),
                        ParameterValue::String("Brand#34".into()),
                        ParameterValue::String("LG CASE".into()),
                        ParameterValue::String("LG BOX".into()),
                        ParameterValue::String("LG PACK".into()),
                        ParameterValue::String("LG PKG".into()),
                        ParameterValue::Number(20),
                        ParameterValue::Number(20),
                        ParameterValue::Number(10),
                        ParameterValue::Number(1),
                        ParameterValue::Number(15),
                        ParameterValue::String("AIR".into()),
                        ParameterValue::String("AIR REG".into()),
                        ParameterValue::String("DELIVER IN PERSON".into()),
                    ]);
                }
                "q20" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("forest%".into()),
                        ParameterValue::Float(0.5),
                        ParameterValue::String("1994-01-01".into()),
                        ParameterValue::String("1994-01-01".into()),
                        ParameterValue::String("CANADA".into()),
                    ]);
                }
                "q21" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("F".into()),
                        ParameterValue::String("SAUDI ARABIA".into()),
                    ]);
                }
                "q22" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("13".into()),
                        ParameterValue::String("31".into()),
                        ParameterValue::String("23".into()),
                        ParameterValue::String("29".into()),
                        ParameterValue::String("30".into()),
                        ParameterValue::String("18".into()),
                        ParameterValue::String("17".into()),
                        ParameterValue::Float(0.00),
                        ParameterValue::String("13".into()),
                        ParameterValue::String("31".into()),
                        ParameterValue::String("23".into()),
                        ParameterValue::String("29".into()),
                        ParameterValue::String("30".into()),
                        ParameterValue::String("18".into()),
                        ParameterValue::String("17".into()),
                    ]);
                }
                _ => {}
            }
            q
        })
        .collect()
}
