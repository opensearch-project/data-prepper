# OpenSearch API Source

This is a source plugin that supports HTTP protocol. It supports [OpenSearch Bulk Document API](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/). All the paths and HTTP methods for [Bulk API operation](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/#path-and-http-methods) are supported. It will also support optional [bulk URL parameters](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/#url-parameters).

## Usages
To get started with OpenSearch API source, create the following `pipeline.yaml` configuration:
```yaml
source:
   opensearch_api:
```

### Response status

* `200`: the request data has been successfully written into the buffer.
* `400`: the request data is either in mal-format or unsupported codec.
* `408`: the request data fails to be written into the buffer within the timeout.
* `413`: the request data size is larger than the configured capacity.
* `429`: the request has been rejected due to the OpenSearch API source executor being in full capacity.

## Configurations

* port (Optional) => An `int` between 0 and 65535 represents the port source is running on. Default is ```9202```.
* path (Optional) => A `string` which represents the URI path for endpoint invocation. It should start with `/` and length should be at least 1. Path can contain `${pipelineName}` placeholder which will be replaced with pipeline name. Default value is `/opensearch`.
* health_check_service (Optional) => A `boolean` that determines if a `/health` endpoint on the defined port will be home to a health check. Default is `false`
* unauthenticated_health_check (Optional) => A `boolean` that determines if the health endpoint will require authentication. This option is ignored if no authentication is defined. Default is `false`
* request_timeout (Optional) => An `int` larger than 0 represents request timeout in millis. Default is ```10_000```. 
* thread_count (Optional) => An `int` larger than 0 represents the number of threads to keep in the ScheduledThreadPool. Default is `200`.
* max_connection_count (Optional) => An `int` larger than 0 represents the maximum allowed number of open connections. Default is `500`.
* max_pending_requests (Optional) => An `int` larger than 0 represents the maximum allowed number of tasks in the ScheduledThreadPool work queue. Default is `1024`.
* authentication (Optional) => An authentication configuration. By default, this runs an unauthenticated server. See below for more information.
* compression (Optional) : The compression type applied on the client request payload. Defaults to `none`. Supported values are: 
  * `none`: no compression 
  * `gzip`: apply GZip de-compression on the incoming request.

### Authentication Configurations

By default, the OpenSearch API source input is unauthenticated.

The following is an example of how to run the server with HTTP Basic authentication:

```yaml
source:
  opensearch_api:
    authentication:
      http_basic:
        username: my-user
        password: my_s3cr3t
```

You can also explicitly disable authentication with:

```yaml
source:
  opensearch_api:
    authentication:
      unauthenticated:
```

This plugin uses pluggable authentication for HTTP servers. To provide custom authentication,
create a plugin which implements [`ArmeriaHttpAuthenticationProvider`](../armeria-common/src/main/java/org/opensearch/dataprepper/armeria/authentication/ArmeriaHttpAuthenticationProvider.java)


### SSL

* ssl(Optional) => A `boolean` that enables TLS/SSL. Default is ```false```.
* ssl_certificate_file(Optional) => A `String` that represents the SSL certificate chain file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Required if `ssl` is set to `true` and `use_acm_certificate_for_ssl` is set to `false`.
* ssl_key_file(Optional) => A `String` that represents the SSL key file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Only decrypted key file is supported. Required if `ssl` is set to `true` and `use_acm_certificate_for_ssl` is set to `false`.
* use_acm_certificate_for_ssl(Optional) : A `boolean` that enables TLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is `false`.
* acm_certificate_arn(Optional) : A `String` that represents the ACM certificate ARN. ACM certificate take preference over S3 or local file system certificate. Required if `use_acm_certificate_for_ssl` is set to `true`.
* acm_private_key_password(Optional): A `String` that represents the ACM private key password which that will be used to decrypt the private key. If it's not provided, a random password will be generated.
* acm_certificate_timeout_millis(Optional) : An `int` that represents the timeout in milliseconds for ACM to get certificates. Default value is `120000`.
* aws_region(Optional) : A `String` that represents the AWS region to use `ACM`, `S3`. Required if `use_acm_certificate_for_ssl` is set to `true` or `ssl_certificate_file` and `ssl_key_file` is `AWS S3`.

### Example to enable SSL using OpenSSL

Create the following OpenSearch API source configuration in your `pipeline.yaml`.

```yaml
source:
  opensearch_api:
       ssl: true
       ssl_certificate_file: "/full/path/to/certfile.crt"
       ssl_key_file: "/full/path/to/keyfile.key"
```

Generate a private key named `keyfile.key`, along with a self-signed certificate file named `certfile.crt`.

```
openssl req  -nodes -new -x509  -keyout keyfile.key -out certfile.crt -subj "/L=test/O=Example Com Inc./OU=Example Com Inc. Root CA/CN=Example Com Inc. Root CA"
```

Make sure to replace the paths for the `ssl_certificate_file` and `ssl_key_file` for the OpenSearch API source configuration with the actual paths of the files.

- Use the following command to send a sample index action on the Bulk API request by setting the index `index = movies` in the body of the request. 

```
curl -k -XPOST -H "Content-Type: application/json" -d '{ "index": { "_index": "movies", "_id": "tt1979320" } }
{ "title": "Rush", "year": 2013}' 
http://localhost:9202/opensearch/_bulk
```

- Alternatively, use the following command to set the index `index = movies` in the path 
```
curl -k -XPOST -H "Content-Type: application/json" -d '{ "index": { "_index": "movies", "_id": "tt1979320" } }
{ "title": "Rush", "year": 2013}' 
http://localhost:9202/opensearch/movies/_bulk
```

# Metrics

### Counter
- `requestsReceived`: measures total number of requests received by `/opensearch` endpoint.
- `requestsRejected`: measures total number of requests rejected (429 response status code) by OpenSearch API source plugin.
- `successRequests`: measures total number of requests successfully processed (200 response status code) by OpenSearch API source plugin.
- `badRequests`: measures total number of requests with invalid content type or format processed by OpenSearch API source plugin (400 response status code).
- `requestTimeouts`: measures total number of requests that time out in the OpenSearch API source server (415 response status code).
- `requestsTooLarge`: measures total number of requests of which the events size in the content is larger than the buffer capacity (413 response status code).
- `internalServerError`: measures total number of requests processed by the OpenSearch API source with custom exception type (500 response status code).

### Timer
- `requestProcessDuration`: measures latency of requests processed by the OpenSearch API source plugin in seconds. 

### Distribution Summary
- `payloadSize`: measures the distribution of incoming requests payload sizes in bytes.

## Developer Guide
This plugin is compatible with Java 14. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
