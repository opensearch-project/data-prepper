/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace.util;

import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility class for deriving fault, error, operation, and environment attributes on SERVER spans.
 * This class contains logic copied from SpanStateData in otel-apm-service-map-processor to ensure
 * consistent behavior for attribute derivation.
 */
public class OTelSpanDerivationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OTelSpanDerivationUtil.class);
    
    // Attribute keys for derived values
    public static final String DERIVED_FAULT_ATTRIBUTE = "derived.fault";
    public static final String DERIVED_ERROR_ATTRIBUTE = "derived.error";
    public static final String DERIVED_OPERATION_ATTRIBUTE = "derived.operation";
    public static final String DERIVED_ENVIRONMENT_ATTRIBUTE = "derived.environment";
    
    private static final String SPAN_KIND_SERVER = "SERVER";
    
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
     * Derive attributes for a single span and add them to the span's attributes
     * 
     * @param span The span to derive attributes for
     */
    private static void deriveAttributesForSpan(final Span span) {
        try {
            final Map<String, Object> spanAttributes = span.getAttributes();
            
            final ErrorFaultResult errorFault = computeErrorAndFault(span.getStatus(), spanAttributes);
            
            final String operationName = computeOperationName(span.getName(), spanAttributes);
            
            final String environment = computeEnvironment(spanAttributes);
            
            span.getAttributes().put(DERIVED_FAULT_ATTRIBUTE, String.valueOf(errorFault.fault));
            span.getAttributes().put(DERIVED_ERROR_ATTRIBUTE, String.valueOf(errorFault.error));
            span.getAttributes().put(DERIVED_OPERATION_ATTRIBUTE, operationName);
            span.getAttributes().put(DERIVED_ENVIRONMENT_ATTRIBUTE, environment);
            
            LOG.debug("Derived attributes for SERVER span {}: fault={}, error={}, operation={}, environment={}", 
                     span.getSpanId(), errorFault.fault, errorFault.error, operationName, environment);
                     
        } catch (Exception e) {
            LOG.warn("Failed to derive attributes for span {}: {}", span.getSpanId(), e.getMessage(), e);
        }
    }
    
    /**
     * Compute error and fault indicators based on span status and HTTP status codes
     * Logic copied from SpanStateData.computeErrorAndFault
     * 
     * @param spanStatusMap The span status map containing status code
     * @param spanAttributes The span attributes containing HTTP status codes
     * @return ErrorFaultResult containing error and fault indicators
     */
    private static ErrorFaultResult computeErrorAndFault(final Map<String, Object> spanStatusMap, final Map<String, Object> spanAttributes) {
        int error = 0;
        int fault = 0;
        
        Integer httpStatusCode = null;
        if (spanAttributes != null) {
            final Object responseStatusCode = spanAttributes.get("http.response.status_code");
            if (responseStatusCode != null) {
                httpStatusCode = parseHttpStatusCode(responseStatusCode);
            } else {
                final Object statusCode = spanAttributes.get("http.status_code");
                if (statusCode != null) {
                    httpStatusCode = parseHttpStatusCode(statusCode);
                }
            }
        }
        
        final boolean hasStatus = isSpanStatusError(spanStatusMap);
        final boolean hasHttpStatus = (httpStatusCode != null);
        
        if (!hasStatus && !hasHttpStatus) {
            error = 0;
            fault = 0;
        } else if (!hasHttpStatus && hasStatus) {
            fault = 1;
            error = 0;
        } else if (hasHttpStatus) {
            if (httpStatusCode >= 500 && httpStatusCode <= 599) {
                fault = 1;
                error = 0;
            } else if (httpStatusCode >= 400 && httpStatusCode <= 499) {
                fault = 0;
                error = 1;
            } else {
                fault = 0;
                error = 0;
            }
        }
        
        return new ErrorFaultResult(error, fault);
    }
    
    /**
     * Parse HTTP status code from various object types
     * Logic copied from SpanStateData.parseHttpStatusCode
     * 
     * @param statusCodeObject The status code object (Integer, String, etc.)
     * @return Parsed integer status code, or null if invalid
     */
    private static Integer parseHttpStatusCode(final Object statusCodeObject) {
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
     * Check if span status indicates an error
     * Logic copied from SpanStateData.isSpanStatusError but adapted for Map status
     * 
     * @param spanStatusMap The span status map containing status code
     * @return true if status indicates error
     */
    private static boolean isSpanStatusError(final Map<String, Object> spanStatusMap) {
        if (spanStatusMap == null) {
            return false;
        }
        
        final Object statusCode = spanStatusMap.get("code");
        if (statusCode == null) {
            return false;
        }
        
        final String statusString = statusCode.toString();
        
        return "ERROR".equalsIgnoreCase(statusString) ||
               "2".equals(statusString) ||
               statusString.toLowerCase().contains("error");
    }
    
    /**
     * Compute operation name using HTTP-aware derivation rules
     * Logic copied from SpanStateData.computeOperationName
     * 
     * @param spanName The span name from the span
     * @param spanAttributes The span attributes containing HTTP method and URL information
     * @return Computed operation name
     */
    private static String computeOperationName(final String spanName, final Map<String, Object> spanAttributes) {
        final String method1 = getStringAttribute(spanAttributes, "http.request.method");
        final String method2 = getStringAttribute(spanAttributes, "http.method");
        
        final boolean useHttpDerivation = spanName == null ||
                                         "UnknownOperation".equals(spanName) || 
                                         (method1 != null && spanName.equals(method1)) ||
                                         (method2 != null && spanName.equals(method2));
        
        if (useHttpDerivation) {
            final String httpMethod = method1 != null ? method1 : method2;
            
            String httpUrl = getStringAttribute(spanAttributes, "http.path");
            if (httpUrl == null) {
                httpUrl = getStringAttribute(spanAttributes, "http.target");
            }
            if (httpUrl == null) {
                httpUrl = getStringAttribute(spanAttributes, "http.url");
            }
            if (httpUrl == null) {
                httpUrl = getStringAttribute(spanAttributes, "url.full");
            }
            
            if (httpMethod == null || httpUrl == null || httpUrl.isEmpty()) {
                return "UnknownOperation";
            }
            
            String path = httpUrl;
            final int queryIndex = path.indexOf('?');
            if (queryIndex != -1) {
                path = path.substring(0, queryIndex);
            }
            final int fragmentIndex = path.indexOf('#');
            if (fragmentIndex != -1) {
                path = path.substring(0, fragmentIndex);
            }
            
            String firstSectionPath = extractFirstPathSection(path);
            
            return httpMethod + " " + firstSectionPath;
        } else {
            return spanName;
        }
    }
    
    /**
     * Extract first section from URL path
     * Logic copied from SpanStateData.extractFirstPathSection
     * 
     * @param path The URL path
     * @return First section of the path (e.g., "/payment/1234" -> "/payment")
     */
    private static String extractFirstPathSection(final String path) {
        if (path == null || path.isEmpty()) {
            return "/";
        }
        
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        
        final int secondSlashIndex = normalizedPath.indexOf('/', 1);
        if (secondSlashIndex == -1) {
            return normalizedPath;
        } else {
            return normalizedPath.substring(0, secondSlashIndex);
        }
    }
    
    /**
     * Compute environment from resource attributes
     * Logic copied from SpanStateData.computeEnvironment
     * 
     * @param spanAttributes The span attributes containing resource information
     * @return Computed environment string
     */
    private static String computeEnvironment(final Map<String, Object> spanAttributes) {
        if (spanAttributes == null) {
            return "generic:default";
        }
        
        final Object resourceObj = spanAttributes.get("resource");
        if (!(resourceObj instanceof Map)) {
            return "generic:default";
        }
        
        @SuppressWarnings("unchecked")
        final Map<String, Object> resource = (Map<String, Object>) resourceObj;
        
        final Object resourceAttributesObj = resource.get("attributes");
        if (!(resourceAttributesObj instanceof Map)) {
            return "generic:default";
        }
        
        @SuppressWarnings("unchecked")
        final Map<String, Object> resourceAttributes = (Map<String, Object>) resourceAttributesObj;
        
        String environmentValue = getStringAttributeFromMap(resourceAttributes, "deployment.environment.name");
        if (isNonEmptyString(environmentValue)) {
            return environmentValue;
        }
        
        environmentValue = getStringAttributeFromMap(resourceAttributes, "deployment.environment");
        if (isNonEmptyString(environmentValue)) {
            return environmentValue;
        }
        
        return "generic:default";
    }
    
    /**
     * Get string attribute from span attributes map
     * Logic copied from SpanStateData.getStringAttribute
     * 
     * @param attributes The span attributes map
     * @param key The attribute key
     * @return String value or null if not present/not a string
     */
    private static String getStringAttribute(final Map<String, Object> attributes, final String key) {
        if (attributes == null) {
            return null;
        }
        
        final Object value = attributes.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Get string attribute from a map safely
     * Logic copied from SpanStateData.getStringAttributeFromMap
     * 
     * @param map The map to get value from
     * @param key The attribute key
     * @return String value or null if not present/not a string
     */
    private static String getStringAttributeFromMap(final Map<String, Object> map, final String key) {
        if (map == null) {
            return null;
        }
        
        final Object value = map.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Check if string is non-empty
     * Logic copied from SpanStateData.isNonEmptyString
     * 
     * @param value The string value to check
     * @return true if string is non-null and non-empty
     */
    private static boolean isNonEmptyString(final String value) {
        return value != null && !value.trim().isEmpty();
    }
    
    /**
     * Simple data class to hold error and fault computation results
     */
    private static class ErrorFaultResult {
        final int error;
        final int fault;
        
        ErrorFaultResult(final int error, final int fault) {
            this.error = error;
            this.fault = fault;
        }
    }
}
