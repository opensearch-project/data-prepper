# otel-v1-apm-span-index-template

## Description
Documents in this index correspond to spans following the [OpenTelemetry tracing specification](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md). Many fields are directly copied from the span, however some fields are derived and not present in the original span.

```json
{
  "version": 0,
  "mappings": {
    "date_detection": false,
    "dynamic_templates": [
      {
        "resource_attributes_map": {
          "mapping": {
            "type":"keyword"
          },
          "path_match":"resource.attributes.*"
        }
      },
      {
        "attributes_map": {
          "mapping": {
            "type":"keyword"
          },
          "path_match":"attributes.*"
        }
      }
    ],
    "_source": {
      "enabled": true
    },
    "properties": {
      "traceId": {
        "ignore_above": 256,
        "type": "keyword"
      },
      "spanId": {
        "ignore_above": 256,
        "type": "keyword"
      },
      "parentSpanId": {
        "ignore_above": 256,
        "type": "keyword"
      },
      "name": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "traceGroup": {
        "ignore_above": 1024,
        "type": "keyword"
      },
      "traceGroupFields": {
        "properties": {
          "endTime": {
            "type": "date_nanos"
          },
          "durationInNanos": {
            "type": "long"
          },
          "statusCode": {
            "type": "integer"
          }
        }
      },
      "kind": {
        "ignore_above": 128,
        "type": "keyword"
      },
      "startTime": {
        "type": "date_nanos"
      },
      "endTime": {
        "type": "date_nanos"
      },
      "status": {
        "properties": {
          "code": {
            "type": "integer"
          },
          "message": {
            "type": "keyword"
          }
        }
      },
      "serviceName": {
        "type": "keyword"
      },
      "durationInNanos": {
        "type": "long"
      },
      "events": {
        "type": "nested",
        "properties": {
          "time": {
            "type": "date_nanos"
          }
        }
      },
      "links": {
        "type": "nested"
      }
    }
  }
}
```

## Fields
Many fields are either copied or derived from the [trace specification protobuf](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto) format.

* traceId - A unique identifier for a trace. All spans from the same trace share the same traceId.
* spanId - A unique identifier for a span within a trace, assigned when the span is created.
* traceState - Conveys information about request position in multiple distributed tracing graphs.
* parentSpanId - The `spanId` of this span's parent span. If this is a root span, then this field must be empty.
* name - A description of the span's operation.
* kind - The type of span. See [OpenTelemetry - SpanKind](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#spankind).
* startTime - The start time of the span.
* endTime - The end time of the span.
* durationInNanos - Difference in nanoseconds between `startTime` and `endTime`.
* serviceName - Currently derived from the `opentelemetry.proto.resource.v1.Resource` associated with the span, the resource from the span originates.
* events - A list of events. See [OpenTelemetry - Events](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#add-events).
* links - A list of linked spans. See [OpenTelemetry - Links](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#specifying-links).
* droppedAttributesCount - The number of attributes that were discarded.
* droppedEventsCount - The number of events that were discarded.
* droppedLinksCount - The number of links that were dropped.
* traceGroup - A derived field, the `name` of the trace's root span.
* traceGroupFields.endTime - A derived field, the `endTime` of the trace's root span.
* traceGroupFields.statusCode - A derived field, the `status.code` of the trace's root span.
* traceGroupFields.durationInNanos - A derived field, the `durationInNanos` of the trace's root span.
* span.attributes.* - All span attributes are split into a list of keywords.
* resource.attributes.* - All resource attributes are split into a list of keywords.
* status.code - The status of the span. See [OpenTelemetry - Status](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#set-status).


## Example Documents

```json
{
  "_index": "otel-v1-apm-span-000006",
  "_type": "_doc",
  "_id": "fe0e3811627189df",
  "_score": 1,
  "_source": {
    "traceId": "0000000000000000856bfa5aeba5ec77",
    "spanId": "fe0e3811627189df",
    "traceState": "",
    "parentSpanId": "856bfa5aeba5ec77",
    "name": "/getcart",
    "kind": "SPAN_KIND_UNSPECIFIED",
    "startTime": "2021-05-18T18:58:44.695Z",
    "endTime": "2021-05-18T18:58:44.760Z",
    "durationInNanos": 65000000,
    "serviceName": "cartservice",
    "events": [],
    "links": [],
    "droppedAttributesCount": 0,
    "droppedEventsCount": 0,
    "droppedLinksCount": 0,
    "traceGroup": "/cart",
    "traceGroupFields.endTime": "2021-05-18T18:58:44.983Z",
    "traceGroupFields.statusCode": 0,
    "traceGroupFields.durationInNanos": 387000000,
    "span.attributes.http@method": "GET",
    "span.attributes.http@url": "http://cartservice/GetCart",
    "span.attributes.instance": "cartservice-d847fdcf5-j6s2f",
    "span.attributes.version": "v5",
    "span.attributes.region": "us-east-1",
    "resource.attributes.service@name": "cartservice",
    "span.attributes.net@host@ip": "172.22.0.8",
    "status.code": 0
  },
  "fields": {
    "startTime": [
      "2021-05-18T18:58:44.695Z"
    ],
    "endTime": [
      "2021-05-18T18:58:44.760Z"
    ]
  }
}
```

