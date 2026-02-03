# OpenTelemetry Span Derivation Utility

## Overview

The `OTelSpanDerivationUtil` class provides shared logic for deriving fault, error, operation, and environment attributes from OpenTelemetry spans. This utility ensures consistent behavior across different processors that need to perform similar derivations.

## Features

- **Error and Fault Detection**: Analyzes HTTP status codes and span status to determine error and fault indicators
- **Operation Name Computation**: Derives operation names using HTTP-aware rules
- **Environment Extraction**: Extracts environment information from resource attributes
- **HTTP Status Code Parsing**: Handles various HTTP status code formats
- **Path Extraction**: Extracts first path section from URLs

## Usage

### For Span Enrichment (Trace Raw Processor)

```java
import org.opensearch.dataprepper.plugins.processor.util.OTelSpanDerivationUtil;

// Process a list of SERVER spans and add derived attributes
List<Span> spans = getSpans();
OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

// Process a single span
Span span = getSpan();
OTelSpanDerivationUtil.deriveAttributesForSpan(span);
```

### For Service Map Metrics (Service Map Processor)

```java
import org.opensearch.dataprepper.plugins.processor.util.OTelSpanDerivationUtil;
import org.opensearch.dataprepper.plugins.processor.util.OTelSpanDerivationUtil.ErrorFaultResult;

// Compute error and fault indicators
ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault(spanStatus, spanAttributes);
int error = result.error;
int fault = result.fault;

// Compute operation name
String operation = OTelSpanDerivationUtil.computeOperationName(spanName, spanAttributes);

// Compute environment
String environment = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);
```

## Derived Attributes

When using `deriveServerSpanAttributes()` or `deriveAttributesForSpan()`, the following attributes are added to spans:

- `derived.fault` - "0" or "1" indicating server fault
- `derived.error` - "0" or "1" indicating client error
- `derived.operation` - Computed operation name
- `derived.environment` - Environment identifier

## Error and Fault Logic

| Condition | Fault | Error |
|-----------|-------|--------|
| HTTP 5xx (500-599) | 1 | 0 |
| HTTP 4xx (400-499) | 0 | 1 |
| Span status error only | 1 | 0 |
| No error indicators | 0 | 0 |

## Operation Name Logic

1. **HTTP Derivation**: Used when span name is null, "UnknownOperation", or matches HTTP method
   - Format: `"{METHOD} {first_path_section}"` (e.g., "GET /payment")
   - Attributes checked: `http.request.method`, `http.method`, `http.path`, `http.target`, `http.url`, `url.full`

2. **Span Name Fallback**: Uses original span name when HTTP derivation doesn't apply

## Environment Logic

Looks for environment in this order:
1. `resource.attributes.deployment.environment.name`
2. `resource.attributes.deployment.environment`
3. Default: `"generic:default"`

## Supported HTTP Status Code Formats

- `Integer` objects
- `Long` objects
- `String` representations of numbers
- Returns `null` for invalid formats

## Public Methods

All core derivation methods are public static methods that can be called directly:

- `computeErrorAndFault(spanStatus, spanAttributes)`
- `computeOperationName(spanName, spanAttributes)`
- `computeEnvironment(spanAttributes)`
- `parseHttpStatusCode(statusCodeObject)`
- `isSpanStatusError(spanStatus)`
- `getStringAttribute(attributes, key)`
- `extractFirstPathSection(path)`

## Dependencies

- Requires `data-prepper-api` for Span interface
- Uses `slf4j` for logging
- No additional dependencies needed