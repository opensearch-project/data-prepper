# OpenTelemetry APM Service Map Processor

## Overview

The `otel_apm_service_map` processor analyzes OpenTelemetry trace spans to automatically generate Application Performance Monitoring (APM) service map relationships and metrics. It creates structured events that can be visualized as service topology graphs, showing how services communicate with each other and their performance characteristics.

## Key Features

- **Service Relationship Discovery**: Automatically identifies service-to-service connections from OpenTelemetry spans
- **APM Metrics Generation**: Creates latency, throughput, and error rate metrics for service interactions
- **Three-Window Processing**: Uses sliding time windows to ensure complete trace context
- **Environment-Aware**: Supports service environment grouping and custom attributes
- **Off-Heap Storage**: Efficient memory usage with MapDB for large-scale processing
- **Real-Time Processing**: Generates service map data as traces are processed

## How It Works

### Three-Window Sliding Architecture

The processor uses three overlapping time windows to ensure complete trace processing:

- **Previous Window**: Completed spans from the previous time period
- **Current Window**: Spans being actively processed
- **Next Window**: Incoming spans for the next time period

This approach ensures that spans from long-running traces that cross window boundaries are properly correlated.

### Two-Phase Processing

#### Phase 1: Span Decoration
1. **CLIENT Span Processing**: Identifies outbound service calls and decorates them with remote service information
2. **SERVER Span Processing**: Processes inbound requests and back-annotates related CLIENT spans

#### Phase 2: Event Generation
1. **ServiceConnection Events**: Represents service-to-service relationships
2. **ServiceOperationDetail Events**: Represents specific operations within services
3. **Metrics Generation**: Creates aggregated performance metrics

### Span Analysis

The processor analyzes different span kinds:
- **CLIENT spans**: Represent outbound calls to other services
- **SERVER spans**: Represent inbound requests being processed
- **Span relationships**: Uses parent-child relationships to build complete call chains

## Configuration

### Basic Configuration

```yaml
processor:
  - otel_apm_service_map:
      window_duration: 60s
      db_path: "data/otel-apm-service-map/"
      group_by_attributes:
        - "service.version"
        - "deployment.environment"
```

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `window_duration` | Duration | `60s` | Fixed time window in seconds for evaluating APM service map relationships |
| `db_path` | String | `"data/otel-apm-service-map/"` | Directory path for database files storing transient processing data |
| `group_by_attributes` | List<String> | `[]` | OpenTelemetry resource attributes to include in service grouping |

### Advanced Configuration

```yaml
processor:
  - otel_apm_service_map:
      window_duration: 120s  # 2-minute windows for high-latency services
      db_path: "/tmp/apm-service-map/"
      group_by_attributes:
        - "service.version"
        - "deployment.environment"
        - "service.namespace"
        - "k8s.cluster.name"
```

## Usage Examples

### Basic Pipeline Configuration

```yaml
version: "2"
otel-apm-service-map-pipeline:
  source:
    otel_trace_source:
      ssl: false
      port: 21890
  route:
      - service_local_details : '/eventType == "ServiceLocalDetails"'
      - service_remote_details : '/eventType == "ServiceRemoteDetails"'
      - service_processed_metrics : '/eventType == "METRIC"'
  processor:
    - otel_apm_service_map:
        window_duration: 60s
        db_path: "data/otel-apm-service-map/"
  sink:
    - opensearch:
        hosts: ["https://localhost:9200"]
        index: "apm-service-map-%{yyyy.MM.dd}"
        username: "admin"
        password: "admin"
        routes: [service_local_details, service_remote_details]
    - prometheus:
        ...
        routes: [service_processed_metrics]
```

### Multi-Environment Setup

```yaml
version: "2"
multi-env-apm-pipeline:
  source:
    otel_trace_source:
      ssl: false
      port: 21890
  route:
      - service_local_details : '/eventType == "ServiceLocalDetails"'
      - service_remote_details : '/eventType == "ServiceRemoteDetails"'
      - service_processed_metrics : '/eventType == "METRIC"'
  processor:
    - otel_apm_service_map:
        window_duration: 90s
        db_path: "data/multi-env-service-map/"
        group_by_attributes:
          - "deployment.environment"
          - "service.version"
          - "service.namespace"
  sink:
    - prometheus:
        ...
        routes: [service_processed_metrics]
    - opensearch:
        hosts: ["https://localhost:9200"]
        index: "apm-service-map-${deployment.environment}-%{yyyy.MM.dd}"
        routes: [service_local_details, service_remote_details]
        index_type: custom
        template_content: |
          {
            "index_patterns": ["apm-service-map-*"],
            "template": {
              "mappings": {
                "properties": {
                  "serviceName": {"type": "keyword"},
                  "environment": {"type": "keyword"},
                  "destinationServiceName": {"type": "keyword"},
                  "destinationEnvironment": {"type": "keyword"}
                }
              }
            }
          }
```

## Output Events

### ServiceConnection Events

Represents a connection between two services:

```json
{
  "eventType": "SERVICE_MAP",
  "data": {
    "service": {
      "keyAttributes": {
        "environment": "production",
        "serviceName": "user-service"
      },
      "groupByAttributes": {
        "service.version": "1.2.3",
        "deployment.environment": "production"
      }
    },
    "destinationService": {
      "keyAttributes": {
        "environment": "production",
        "serviceName": "auth-service"
      },
      "groupByAttributes": {
        "service.version": "2.1.0"
      }
    },
    "timestamp": "2023-12-01T12:00:00Z"
  }
}
```

### ServiceOperationDetail Events

Represents specific operations within a service:

```json
{
  "eventType": "SERVICE_MAP",
  "data": {
    "service": {
      "keyAttributes": {
        "environment": "production", 
        "serviceName": "auth-service"
      },
      "groupByAttributes": {
        "service.version": "2.1.0"
      }
    },
    "operation": {
      "operationName": "authenticate",
      "destinationService": {
        "keyAttributes": {
          "environment": "production",
          "serviceName": "database-service"
        }
      },
      "destinationOperation": "query"
    },
    "timestamp": "2023-12-01T12:00:00Z"
  }
}
```

### Generated Metrics

The processor also generates time-series metrics:

- **Latency metrics**: `latency_histogram` with percentiles
- **Throughput metrics**: `request_count` and `request_rate` 
- **Error metrics**: `error_count` and `error_rate`
- **Status code metrics**: HTTP status code distributions

## Performance Considerations

### Memory Usage

- **Off-heap storage**: Uses MapDB to store span state data outside JVM heap
- **Window size impact**: Larger `window_duration` values require more storage
- **Trace volume**: Memory usage scales with the number of concurrent traces

### Storage Requirements

- **Database path**: Ensure sufficient disk space at the configured `db_path`
- **Cleanup**: Old database files are automatically cleaned up during window rotation
- **I/O performance**: Use fast storage (SSD) for better performance

### Monitoring Metrics

The processor exposes the following metrics for monitoring:

- `spansDbSize`: Total size of span databases in bytes
- `spansDbCount`: Total number of spans stored across all databases

### With OpenSearch Dashboards

Create index patterns and visualizations:

1. **Index Pattern**: `apm-service-map-*`
2. **Service Map Visualization**: Network graph showing service connections
3. **Metrics Dashboard**: Time-series charts for latency, throughput, and errors

## Related Documentation

- [OpenTelemetry Trace Processing](../otel-trace-raw-processor/README.md)  
- [Service Map State Management](../service-map-stateful/README.md)
