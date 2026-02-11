/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.utils;

import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.model.metric.JacksonMetric.ATTRIBUTES_KEY;

/**
 * Utility class for deriving fault, error, operation, and environment attributes from OpenTelemetry spans.
 * This class provides shared logic that can be used by multiple processors for consistent attribute derivation.
 *
 * Originally extracted from SpanStateData in otel-apm-service-map-processor to ensure
 * consistent behavior across different processors.
 */
public class OTelSpanDerivationUtil {
    // Attribute keys for derived values
    public static final String DERIVED_FAULT_ATTRIBUTE = "derived.fault";
    public static final String DERIVED_ERROR_ATTRIBUTE = "derived.error";
    public static final String DERIVED_OPERATION_ATTRIBUTE = "derived.operation";
    public static final String DERIVED_ENVIRONMENT_ATTRIBUTE = "derived.environment";
    private static final Logger LOG = LoggerFactory.getLogger(OTelSpanDerivationUtil.class);
    private static final String SPAN_KIND_SERVER = "SPAN_KIND_SERVER";

    /**
     * Derives fault, error, operation, and environment attributes for SERVER spans in the provided list.
     * Only SERVER spans (kind == SERVER) will be decorated with derived attributes.
     *
     * @param spans List of spans to process
     */
    public static void deriveServerSpanAttributes(final List<Span> spans) {
        if (spans == null) {
            return;
        }

        for (final Span span : spans) {
            if (span != null && SPAN_KIND_SERVER.equals(span.getKind())) {
                deriveAttributesForSpan(span);
            }
        }
    }


    /**
     * Adds an attribute to the span. This method just delegates to span.putAttribute()
     * since JacksonSpan already handles null/immutable maps correctly.
     *
     * @param span The span to add the attribute to
     * @param key The attribute key
     * @param value The attribute value
     */
    private static void putAttribute(final Span span, final String key, final Object value) {
        Map<String, Object> attributes = span.getAttributes();
        if (attributes == null) {
            attributes = new HashMap<>();
        } else {
            attributes = new HashMap<>(attributes);
        }
        attributes.put(key, value);
        span.put(ATTRIBUTES_KEY ,attributes);
    }

    /**
     * Derive attributes for a single span and add them to the span's attributes
     *
     * @param span The span to derive attributes for
     */
    public static void deriveAttributesForSpan(final Span span) {
        try {
            final Map<String, Object> spanAttributes = span.getAttributes();

            final ErrorFaultResult errorFault = computeErrorAndFault(span.getStatus(), spanAttributes);

            final String operationName = computeOperationName(span.getName(), spanAttributes);

            final String environment = computeEnvironment(spanAttributes);

            // Add derived attributes using our safe attribute setting method
            putAttribute(span, DERIVED_FAULT_ATTRIBUTE, String.valueOf(errorFault.fault));
            putAttribute(span, DERIVED_ERROR_ATTRIBUTE, String.valueOf(errorFault.error));
            putAttribute(span, DERIVED_OPERATION_ATTRIBUTE, operationName);
            putAttribute(span, DERIVED_ENVIRONMENT_ATTRIBUTE, environment);

            LOG.debug("Derived attributes for SERVER span {}: fault={}, error={}, operation={}, environment={}",
                    span.getSpanId(), errorFault.fault, errorFault.error, operationName, environment);

        } catch (Exception e) {
            LOG.warn("Failed to derive attributes for span {}: {}", span.getSpanId(), e.getMessage(), e);
        }
    }

    /**
     * Compute error and fault indicators based on span status and HTTP status codes.
     * Package-private for testing purposes only.
     *
     * @param spanStatus     The span status (Map with "code" or String like "ERROR")
     * @param spanAttributes The span attributes containing HTTP status codes
     * @return ErrorFaultResult containing error and fault indicators
     */
    public static ErrorFaultResult computeErrorAndFault(final Object spanStatus, final Map<String, Object> spanAttributes) {
        // Check HTTP status first (highest priority)
        if (spanAttributes != null) {
            Object httpStatusObj = spanAttributes.get("http.response.status_code");
            if (httpStatusObj == null) {
                httpStatusObj = spanAttributes.get("http.status_code");
            }

            if (httpStatusObj != null) {
                Integer httpStatusCode = parseHttpStatusCode(httpStatusObj);
                if (httpStatusCode != null) {
                    if (httpStatusCode >= 500) return new ErrorFaultResult(0, 1); // 5xx = fault
                    if (httpStatusCode >= 400) return new ErrorFaultResult(1, 0); // 4xx = error
                    return new ErrorFaultResult(0, 0); // 2xx/3xx = success
                }
            }
        }

        // No HTTP status, check span status for error
        if (isSpanStatusError(spanStatus)) {
            return new ErrorFaultResult(0, 1); // span error = fault
        }

        return new ErrorFaultResult(0, 0); // no error/fault
    }

    /**
     * Parse HTTP status code from various object types.
     * Package-private for testing purposes only.
     *
     * @param statusCodeObject The status code object (Integer, String, etc.)
     * @return Parsed integer status code, or null if invalid
     */
    static Integer parseHttpStatusCode(final Object statusCodeObject) {
        if (statusCodeObject == null) {
            return null;
        }

        try {
            if (statusCodeObject instanceof Integer) {
                return (Integer) statusCodeObject;
            } else if (statusCodeObject instanceof Long) {
                return ((Long) statusCodeObject).intValue();
            } else {
                return Integer.parseInt(statusCodeObject.toString());
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Check if span status indicates an error.
     * Package-private for testing purposes only.
     *
     * @param spanStatus The span status (Map with "code" key or String like "ERROR"/"2")
     * @return true if status indicates error
     */
    private static boolean isSpanStatusError(final Object spanStatus) {
        if (spanStatus == null) return false;

        String statusString;
        if (spanStatus instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> statusMap = (Map<String, Object>) spanStatus;
            final Object code = statusMap.get("code");
            if (code == null) return false;
            statusString = code.toString();
        } else {
            statusString = spanStatus.toString();
        }

        return "ERROR".equalsIgnoreCase(statusString) ||
               "2".equals(statusString) ||
               statusString.toLowerCase().contains("error");
    }

    /**
     * Compute operation name using HTTP-aware derivation rules.
     * Package-private for testing purposes only.
     *
     * @param spanName       The span name from the span
     * @param spanAttributes The span attributes containing HTTP method and URL information
     * @return Computed operation name
     */
    public static String computeOperationName(final String spanName, final Map<String, Object> spanAttributes) {
        // Get HTTP method (try new standard first, then legacy)
        String method = getStringAttribute(spanAttributes, "http.request.method");
        if (method == null) {
            method = getStringAttribute(spanAttributes, "http.method");
        }

        // Get HTTP path (try multiple attributes)
        String path = getStringAttribute(spanAttributes, "http.path");
        if (path == null) path = getStringAttribute(spanAttributes, "http.target");
        if (path == null) path = getStringAttribute(spanAttributes, "http.url");
        if (path == null) path = getStringAttribute(spanAttributes, "url.full");

        // Use HTTP method + first path section when available
        if (method != null && path != null && !path.isEmpty() &&
            (spanName == null || "UnknownOperation".equals(spanName) || spanName.equals(method))) {

            // Extract first path section
            String cleanPath = path.split("[?#]")[0]; // Remove query/fragment
            if (!cleanPath.startsWith("/")) cleanPath = "/" + cleanPath;
            int secondSlash = cleanPath.indexOf('/', 1);
            String firstPath = (secondSlash > 0) ? cleanPath.substring(0, secondSlash) : cleanPath;

            return method + " " + firstPath;
        }

        // If span name equals method but no path available, return UnknownOperation
        if (method != null && spanName != null && spanName.equals(method)) {
            return "UnknownOperation";
        }

        // Fall back to span name or default
        return spanName != null ? spanName : "UnknownOperation";
    }

    /**
     * Compute environment from resource attributes.
     * Package-private for testing purposes only.
     *
     * @param spanAttributes The span attributes containing resource information
     * @return Computed environment string
     */
    public static String computeEnvironment(final Map<String, Object> spanAttributes) {
        try {
            // Navigate: spanAttributes -> "resource" -> "attributes" -> deployment keys
            @SuppressWarnings("unchecked")
            Map<String, Object> resourceAttrs = (Map<String, Object>)
                ((Map<String, Object>) spanAttributes.get("resource")).get("attributes");

            // Extract from resource.attributes.deployment.environment.name
            String env = getStringAttribute(resourceAttrs, "deployment.environment.name");
            if (env != null && !env.trim().isEmpty()) {
                return env;
            }

            // Fall back to resource.attributes.deployment.environment
            env = getStringAttribute(resourceAttrs, "deployment.environment");
            if (env != null && !env.trim().isEmpty()) {
                return env;
            }
        } catch (Exception ignored) {
            // Any navigation failure falls through to default
        }

        // Default: 'generic:default'
        return "generic:default";
    }

    /**
     * Get string attribute from a map safely.
     * Package-private for testing purposes only.
     *
     * @param map The map to get value from
     * @param key The attribute key
     * @return String value or null if not present/not a string
     */
    static String getStringAttribute(final Map<String, Object> map, final String key) {
        if (map == null) {
            return null;
        }

        final Object value = map.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * @deprecated Use {@link #getStringAttribute(Map, String)} instead.
     * This method is kept for backward compatibility.
     */
    @Deprecated
    static String getStringAttributeFromMap(final Map<String, Object> map, final String key) {
        return getStringAttribute(map, key);
    }


    /**
     * Simple data class to hold error and fault computation results.
     */
    public static class ErrorFaultResult {
        final int error;
        final int fault;

        public ErrorFaultResult(final int error, final int fault) {
            this.error = error;
            this.fault = fault;
        }

        public int getError() {
            return error;
        }

        public int getFault() {
            return fault;
        }
    }
}
