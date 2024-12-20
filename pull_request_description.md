### Description

Introduce http endpoint to support otl/http for the otel trace source. So far, this draft PR contains:

- A single armeria server listening on a port
- A HTTP Service
  - listening under `/opentelemetry.proto.collector.trace.v1.TraceService/Export`
  - processing ExportTraceServiceRequest
  - supporting basic auth/tls
- (not yet complete)Testing of the http service

This draft PR serves as starting for further discussions:

# Configuration of HTTP and gRPC Service

Currently, the source config is responsible for setting up a working gRPC service. Now, an HTTP Service has to be configured as well.

As far as this PR is concerned, we can assign the current config items into two groups:

- Config items for gRPC specific features (e.g. `unframed_requests`, `proto_reflection_service`)
- Config items relevant for both services (e.g. `compression`, `authentication`) and should/could 

This leads to the questions how the structure of the config should look like in the future

**Create distinct sections for every endpoint**

```yaml
port: 123
thread_count: 123
...
http:
  path: /path
  compression: gzip
  authentication:
    http_basic:
grpc:
  compression: gzip
  proto_reflection_service: true
  authentication:
    http_basic:
```

**Let the endpoint share as much of the current config as possible**

e.g. features like `compression`, `authentication`


# Do we want to keep unframed requests

My understanding is that unframed requests enable clients to send plain http requests to the gRPC endpoint and would be rendered deprecated by this PR. However, there might be more to this feature which I'm currently unaware of.

 
### Issues Resolved
Resolves #4983

Related to #5259
 
### Check List
- [ ] New functionality includes testing.
- [ ] New functionality has a documentation issue. Please link to it in this PR.
  - [ ] New functionality has javadoc added
- [ ] Commits are signed with a real name per the DCO

By submitting this pull request, I confirm that my contribution is made under the terms of the Apache 2.0 license.
For more information on following Developer Certificate of Origin and signing off your commits, please check [here](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md).
