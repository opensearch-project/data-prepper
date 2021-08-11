# otel-v1-apm-service-map-index-template

## Description
Documents in this index correspond to edges in a service map. Edges are created when a request crosses service boundaries. Documents will exclusively contain either a _destination_ or a _target_:
* Destination: corresponds to a client span calling another service. The _destination_ is the other service being called.
* Target: corresponds to a server span. The _target_ is the operation or API being called by the client.

```json
{
  "version": 0,
  "mappings": {
    "date_detection": false,
    "dynamic_templates": [
      {
        "strings_as_keyword": {
          "mapping": {
            "ignore_above": 1024,
            "type": "keyword"
          },
          "match_mapping_type": "string"
        }
      }
    ],
    "_source": {
      "enabled": true
    },
    "properties": {
      "hashId": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "serviceName": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "kind": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "destination": {
        "properties": {
          "domain": {
            "ignore_above": 1024,
            "type": "keyword"
          },
          "resource": {
            "ignore_above": 1024,
            "type": "keyword"
          }
        }
      },
      "target": {
        "properties": {
          "domain": {
            "ignore_above": 1024,
            "type": "keyword"
          },
          "resource": {
            "ignore_above": 1024,
            "type": "keyword"
          }
        }
      },
      "traceGroupName": {
        "ignore_above": 1024,
        "type": "keyword"
      }
    }
  }
}
```

## Fields
* hashId - A deterministic hash of this relationship.
* kind - The span kind, corresponding to the source of the relationship. See [OpenTelemetry - SpanKind](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#spankind).
* serviceName - The name of the service which emitted the span. Currently derived from the `opentelemetry.proto.resource.v1.Resource` associated with the span.
* destination.domain - The serviceName of the service being called by this client.
* destination.resource - The span name (API, operation, etc.) being called by this client.
* target.domain - The serviceName of the service being called by a client.
* target.resource - The span name (API, operation, etc.) being called by a client.
* traceGroupName - The top-level span name which started the request chain.

## Example Documents
The two example documents below illustrate the "inventory" service calling the "database" service's `updateItem` API. 
```json
{
  "_index": "otel-v1-apm-service-map",
  "_type": "_doc",
  "_id": "7/jRp2VF7544pBN6+mK2vw==",
  "_score": 1,
  "_source": {
    "serviceName": "inventory",
    "kind": "SPAN_KIND_CLIENT",
    "destination": {
      "resource": "updateItem",
      "domain": "database"
    },
    "target": null,
    "traceGroupName": "client_checkout",
    "hashId": "7/jRp2VF7544pBN6+mK2vw=="
  }
}
```

```json
{
  "_index": "otel-v1-apm-service-map",
  "_type": "_doc",
  "_id": "lZcUyuhGYfnaQqt+r73njA==",
  "_version": 3,
  "_score": 0,
  "_source": {
    "serviceName": "database",
    "kind": "SPAN_KIND_SERVER",
    "destination": null,
    "target": {
      "resource": "updateItem",
      "domain": "database"
    },
    "traceGroupName": "client_checkout",
    "hashId": "lZcUyuhGYfnaQqt+r73njA=="
  }
}
```

