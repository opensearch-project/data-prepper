# Core Data Prepper APIs

All Data Prepper instances expose a server with some control APIs. By default, this server runs
on port 4900. Some plugins, especially Source plugins may expose other servers. These will be
on different ports and their configurations are independent of the core API.

For example, to shut down Data Prepper, you can run:

```
curl -X POST http://localhost:4900/shutdown
```

## APIs

The following APIs are available:

```
GET /list
POST /list
```
* lists running pipelines

```
POST /shutdown
```
* starts a graceful shutdown of the Data Prepper

```
GET /metrics/prometheus
POST /metrics/prometheus
```
* returns a scrape of the Data Prepper metrics in Prometheus text format. This API is available provided
      `metrics_registries` parameter in data prepper configuration file `data-prepper-config.yaml` has `Prometheus` as one
      of the registry

```
GET /metrics/sys
POST /metrics/sys
```
* returns JVM metrics in Prometheus text format. This API is available provided `metrics_registries` parameter in data
      prepper configuration file `data-prepper-config.yaml` has `Prometheus` as one of the registry

## Configuring the Server

You can configure your Data Prepper core APIs through the `data-prepper-config.yaml` file. 

### SSL/TLS Connection

Many of the Getting Started guides in this project disable SSL on the endpoint.

```yaml
ssl: false
```

To enable SSL on your Data Prepper endpoint, configure your `data-prepper-config.yaml`
with the following:

```yaml
ssl: true
key_store_file_path: "/usr/share/data-prepper/keystore.p12"
key_store_password: "secret"
private_key_password: "secret"
```

For more information on configuring your Data Prepper server with SSL, see [Server Configuration](https://github.com/opensearch-project/data-prepper/blob/main/docs/configuration.md#server-configuration). 

If you are using a self-signed certificate, you can add the `-k` flag to quickly test out sending curl requests for the core APIs with SSL.

```
curl -k -X POST https://localhost:4900/shutdown
```

### Authentication

The Data Prepper Core APIs support HTTP Basic authentication.
You can set the username and password with the following
configuration in `data-prepper-config.yaml`:

```yaml
authentication:
  http_basic:
    username: "myuser"
    password: "mys3cr3t"
```

You can disable authentication of core endpoints using the following
configuration. Use this with caution because the shutdown API and
others will be accessible to anybody with network access to
your Data Prepper instance.

```yaml
authentication:
  unauthenticated:
```

### Peer Forwarder
Peer forwarder can be configured to enable stateful aggregation across multiple Data Prepper nodes. For more information on configuring Peer Forwarder, see [Peer Forwarder Configuration](https://github.com/opensearch-project/data-prepper/blob/main/docs/peer_forwarder.md).
It is supported by `service_map`, `otel_traces` and `aggregate` processors.

### Shutdown Timeouts
When the DataPrepper `shutdown` API is invoked, the sink and processor `ExecutorService`'s are given time to gracefully shutdown and clear any in-flight data. The default graceful shutdown timeout for these `ExecutorService`'s is 10 seconds. This can be configured with the following optional parameters:

```yaml
processor_shutdown_timeout: "PT15M"
sink_shutdown_timeout: 30s
```

The values for these parameters are parsed into a `Duration` object via the [DataPrepperDurationDeserializer](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-core/src/main/java/org/opensearch/dataprepper/parser/DataPrepperDurationDeserializer.java).
