# Log HTTP Source

This is a source plugin that supports HTTP protocol. Currently ONLY support Json UTF-8 codec for incoming request, e.g. 
`[{"key1": "value1"}, {"key2": "value2"}]`.


## Usages
Currently, we are exposing `/log/ingest` URI path for http log ingestion. Example `.yaml` configuration:
```yaml
source:
   http:
```

### Response status

* `200`: the request data has been successfully written into the buffer.
* `400`: the request data is either in mal-format or unsupported codec.
* `413`: the request data size is larger than the configured capacity.
* `415`: the request fails to be written into the buffer within the timeout.
* `429`: the request has been rejected due to the HTTP source executor being in full capacity.

## Configurations

* port(Optional) => An `int` between 0 and 65535 represents the port source is running on. Default is ```2021```.
* request_timeout(Optional) => An `int` larger than 0 represents request timeout in millis. Default is ```10_000```. 
* thread_count(Optional) => An `int` larger than 0 represents the number of threads to keep in the ScheduledThreadPool. Default is `200`.
* max_connection_count(Optional) => An `int` larger than 0 represents the maximum allowed number of open connections. Default is `500`.
* max_pending_requests(Optional) => An `int` larger than 0 represents the maximum allowed number of tasks in the ScheduledThreadPool work queue. Default is `1024`.
* authentication(Optional) => An authentication configuration. By default, this runs an unauthenticated server. See below for more information.

### Authentication Configurations

By default, the HTTP source input is unauthenticated.

The following is an example of how to run the server with HTTP Basic authentication:

```yaml
source:
  http:
    authentication:
      http_basic:
        username: my-user
        password: my_s3cr3t
```

You can also explicitly disable authentication with:

```yaml
source:
  http:
    authentication:
      unauthenticated:
```

This plugin uses pluggable authentication for HTTP servers. To provide custom authentication,
create a plugin which implements [`ArmeriaHttpAuthenticationProvider`](../armeria-common/src/main/java/com/amazon/dataprepper/armeria/authentication/ArmeriaHttpAuthenticationProvider.java)


### SSL

* ssl(Optional) => A `boolean` enables TLS/SSL. Default is ```false```.
* ssl_certificate_file(Optional) => A `String` represents the SSL certificate chain file path. Required if ```ssl``` is set to ```true```.
* ssl_key_file(Optional) => A `String` represents the SSL key file path. Only decrypted key file is supported. Required if ```ssl``` is set to ```true```.

### Example to enable SSL using OpenSSL

Create the following http source configuration in your `pipeline.yaml`.

```yaml
source:
   http:
       ssl: true
       ssl_certificate_file: "/full/path/to/certfile.crt"
       ssl_key_file: "/full/path/to/keyfile.key"
```

Generate a private key named `keyfile.key`, along with a self-signed certificate file named `certfile.crt`.

```
openssl req  -nodes -new -x509  -keyout keyfile.key -out certfile.crt -subj "/L=test/O=Example Com Inc./OU=Example Com Inc. Root CA/CN=Example Com Inc. Root CA"
```

Make sure to replace the paths for the `ssl_certificate_file` and `ssl_key_file` for the http source configuration with the actual paths of the files.

Send a sample log with the following https curl command

```
curl -k -XPOST -H "Content-Type: application/json" -d '[{"log": "sample log"}]' https://localhost:2021/log/ingest
```

# Metrics

### Counter
- `requestsReceived`: measures total number of requests received by `/log/ingest` endpoint.
- `requestsRejected`: measures total number of requests rejected (429 response status code) by HTTP source plugin.
- `successRequests`: measures total number of requests successfully processed (200 response status code) by HTTP source plugin.
- `badRequests`: measures total number of requests with invalid content type or format processed by HTTP source plugin (400 response status code).
- `requestTimeouts`: measures total number of requests that time out in the HTTP source server (415 response status code).
- `requestsTooLarge`: measures total number of requests of which the events size in the content is larger than the buffer capacity (413 response status code).
- `internalServerError`: measures total number of requests processed by the HTTP source with custom exception type (500 response status code).

### Timer
- `requestProcessDuration`: measures latency of requests processed by the HTTP source plugin in seconds. 

### Distribution Summary
- `payloadSize`: measures the distribution of incoming requests payload sizes in bytes.

## Developer Guide
This plugin is compatible with Java 14. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
