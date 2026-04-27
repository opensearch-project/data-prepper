# Security requirements for push-based sources

Data Prepper supports push-based sources which open networking ports.
This page documents security requirements for creating these sources.

### Framework

For consistency, push-based sources should use Armeria.

Additionally, they should use code for common server creation.
For example, using the [CreateServer](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/http-common/src/main/java/org/opensearch/dataprepper/plugins/server/CreateServer.java) 
class to create a server.

### Authentication

Push-based sources mush use the existing authentication plugins for Armeria.

* [ArmeriaHttpAuthenticationProvider](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/armeria-common/src/main/java/org/opensearch/dataprepper/armeria/authentication/ArmeriaHttpAuthenticationProvider.java)
* [GrpcAuthenticationProvider](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/armeria-common/src/main/java/org/opensearch/dataprepper/armeria/authentication/GrpcAuthenticationProvider.java)

### SSL

Endpoints must enable SSL by default. They should log a warning if SSL is disabled.

### Testing

All push-based sources must include automated tests for:

* **Unauthenticated Access**: Verify that requests without credentials are rejected with HTTP 401
* **Unauthorized Access**: Verify that requests with valid credentials but insufficient permissions are rejected with HTTP 403
* **Authenticated Access**: Verify that properly authenticated requests succeed

Additionally, the test must cover both gRPC and HTTP access.

Some examples:

* [`http` source](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/http-source/src/test/java/org/opensearch/dataprepper/plugins/source/loghttp/HTTPSourceTest.java)
* [`otlp` source](https://github.com/opensearch-project/data-prepper/blob/5ad289dd00cfaa73509c7b0fdb757b73d0f18a0c/data-prepper-plugins/otlp-source/src/test/java/org/opensearch/dataprepper/plugins/source/otlp/OTLPSourceTest.java)

## Push request security checklist

All pull requests for push-based sources should include evaluation against this checklist.

* [ ] Does this PR add or modify an HTTP endpoint?
* [ ] Is the source using an existing web framework within Data Prepper? If not, why not?
* [ ] Does the source support Data Prepper authentication plugins?
* [ ] Are there tests for unauthenticated access rejection?
* [ ] Are there tests for unauthorized access rejection?
* [ ] Are there tests for authenticated access?
* [ ] Do the tests cover all supported protocols (e.g. HTTP and gRPC)?
