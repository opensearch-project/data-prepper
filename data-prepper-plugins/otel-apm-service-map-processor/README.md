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
      window_duration: 60
      db_path: "data/otel-apm-service-map/"
      group_by_attributes:
        - "service.version"
        - "deployment.environment"
```

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `window_duration` | Integer | `60` | Fixed time window in seconds for evaluating APM service map relationships |
| `db_path` | String | `"data/otel-apm-service-map/"` | Directory path for database files storing transient processing data |
| `group_by_attributes` | List<String> | `[]` | OpenTelemetry resource attributes to include in service grouping |

### Advanced Configuration

```yaml
processor:
  - otel_apm_service_map:
      window_duration: 120  # 2-minute windows for high-latency services
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
  processor:
    - otel_apm_service_map:
        window_duration: 60
        db_path: "data/otel-apm-service-map/"
  sink:
    - opensearch:
        hosts: ["https://localhost:9200"]
        index: "apm-service-map-%{yyyy.MM.dd}"
        username: "admin"
        password: "admin"
```

### Multi-Environment Setup

```yaml
version: "2"
multi-env-apm-pipeline:
  source:
    otel_trace_source:
      ssl: false
      port: 21890
  processor:
    - otel_apm_service_map:
        window_duration: 90
        db_path: "data/multi-env-service-map/"
        group_by_attributes:
          - "deployment.environment"
          - "service.version"
          - "service.namespace"
  sink:
    - opensearch:
        hosts: ["https://localhost:9200"]
        index: "apm-service-map-${deployment.environment}-%{yyyy.MM.dd}"
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
  "eventType": "OTelAPMServiceMap",
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
  "eventType": "OTelAPMServiceMap",
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

### Scaling Guidelines

###### TODO : Correct memory allocation based on performance test results

| Trace Volume | Memory Allocation |
|--------------|-------------------|
| < 10k spans/sec | 2-4 GB heap |
| 10k-50k spans/sec | 4-8 GB heap |
| > 50k spans/sec | 8+ GB heap |

## Troubleshooting

### Common Issues

#### High Memory Usage

**Symptoms**: OutOfMemoryError, frequent garbage collection
**Solutions**:
- Increase JVM heap size
- Reduce `window_duration`
- Check for trace data without proper parent-child relationships
- Monitor database file sizes

```bash
# Check database sizes
ls -lh data/otel-apm-service-map/
```

#### Missing Service Connections

**Symptoms**: Incomplete service map, missing edges between services
**Solutions**:
- Verify spans have proper `span.kind` attributes (CLIENT/SERVER)
- Check parent-child span relationships in traces
- Ensure `service.name` is populated on all spans
- Verify trace sampling isn't dropping related spans

#### Database Errors

**Symptoms**: MapDB related exceptions, file corruption
**Solutions**:
- Check disk space at `db_path` location
- Ensure write permissions for Data Prepper process
- Verify no other processes are accessing the database files

```bash
# Check disk space
df -h /path/to/db_path

# Check permissions  
ls -la data/otel-apm-service-map/
```

### Debug Configuration

Enable debug logging for detailed processing information:

```yaml
logging:
  level:
    org.opensearch.dataprepper.plugins.processor.OtelApmServiceMapProcessor: DEBUG
    org.opensearch.dataprepper.plugins.processor.utils.ApmServiceMapMetricsUtil: DEBUG
```

### Monitoring Metrics

The processor exposes the following metrics for monitoring:

- `spansDbSize`: Total size of span databases in bytes
- `spansDbCount`: Total number of spans stored across all databases

## Integration Examples

### With OpenSearch Dashboards

Create index patterns and visualizations:

1. **Index Pattern**: `apm-service-map-*`
2. **Service Map Visualization**: Network graph showing service connections
3. **Metrics Dashboard**: Time-series charts for latency, throughput, and errors

## Best Practices

1. **Window Duration**: Choose based on your longest-running traces
2. **Group-by Attributes**: Include environment and version for better service categorization  
3. **Index Templates**: Use appropriate mapping for service name fields
4. **Monitoring**: Set up alerts on database size and processing metrics
5. **Storage**: Use dedicated storage for database files in high-volume environments

## Related Documentation

- [Data Prepper Processor Configuration](../../README.md)
- [OpenTelemetry Trace Processing](../otel-trace-raw-processor/README.md)  
- [Service Map State Management](../service-map-stateful/README.md)
