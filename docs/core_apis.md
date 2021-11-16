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

* /list
    * lists running pipelines
* /shutdown
    * starts a graceful shutdown of the Data Prepper
* /metrics/prometheus
    * returns a scrape of the Data Prepper metrics in Prometheus text format. This API is available provided
      `metricsRegistries` parameter in data prepper configuration file `data-prepper-config.yaml` has `Prometheus` as one
      of the registry
* /metrics/sys
    * returns JVM metrics in Prometheus text format. This API is available provided `metricsRegistries` parameter in data
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
keyStoreFilePath: "/usr/share/data-prepper/keystore.p12"
keyStorePassword: "secret"
privateKeyPassword: "secret"
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
