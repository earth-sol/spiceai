# DR: Adding additional fields to the Anonymous Metrics

## Context

Spice reports certain metrics anonymously, such as query count, bytes processed, bytes returned, query duration,
and query execution duration. 

## Assumptions

1. Adding the new fields does not impose a significant performance impact on Spice

## Options

## First-Principles

- **Bring data and ML to your application**: Better data means better decisions and a better user experience

## Decision

Spice will add the following new fields to the metrics:

* `user-agent`: The originating User agent of the request. For example, SDKs report values such as `gospice` for the Go SDK or `spicepy` for the Python SDK
* `user-agent-version`: The version of the originating library
* `user-agent-os`: The operating system and architecture of the originating request
* `spice-internal`: An optional field denoting whether this metric is being generated as part of any Spice internal benchmarks or tests
* `build`: `dev` or `release` denoting if the current runtime is built in Release mode or not. 

**Why**:

- As Spice grows and adds new features, the need to have more visibility into usage becomes more beneficial.
- Being able to separate internal Spice benchmarks versus actual user consumption helps to understand growth
- Understanding which SDKs are used most will allow us to focus our efforts.
- Any performance regressions specific to certain SDKs will be easier to identify.

**Why not**:

There may be performance overhead when adding these new metrics, however small. There is also the concern about malformed or incorrect user agent strings, which should be accounted for

## Consequences

- Client SDKs have User Agent values added to all communication with the Spice runtime
- Spice Runtime propagates these metrics to the anonymous metrics reporting
