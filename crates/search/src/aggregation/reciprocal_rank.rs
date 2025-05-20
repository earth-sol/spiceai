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

use super::CandidateAggregation;
use super::Result;
use crate::Error;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;

#[derive(Default)]
pub struct ReciprocalRankFusion;

// SELECT
//   TRUNC((1.0 / (bm25.rank + 60)) + (1.0 / (vector.rank + 60)), 6) as final_rank,
//   bm25.rank as bm25_rank,
//   vector.rank as vector_rank,
//   bm25.title
// FROM
//   bm25,
//   vector
// WHERE
//   bm25.title = vector.title
// ORDER BY final_rank DESC

#[async_trait]
impl CandidateAggregation for ReciprocalRankFusion {
    async fn aggregate(
        &self,
        mut candidate_sets: Vec<SendableRecordBatchStream>,
        primary_key: Vec<String>,
        limit: usize,
    ) -> Result<SendableRecordBatchStream> {
        if candidate_sets.len() == 1 {
            return candidate_sets.pop().ok_or(Error::InternalError {
                source: Box::from(format!(
                    "No search candidates provided to reciprocal rank fusion aggregation."
                )),
            });
        }

        // let ctx = SessionContext::new();
        // let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        // ctx.register_batch(

        Err(Error::InternalError {
            source: Box::from(format!("Only RRF is for a single candidate")),
        })
    }
}
