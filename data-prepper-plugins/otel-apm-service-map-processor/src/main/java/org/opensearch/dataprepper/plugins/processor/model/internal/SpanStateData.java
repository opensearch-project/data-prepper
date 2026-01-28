/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.model.internal;

import org.apache.commons.codec.binary.Hex;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

// TODO : 1. Add new rules as per Producer/Consumers/LocalRoot
// TODO : 2. Move OTelSpanDerivationUtil class to common location and re-use it here.
public class SpanStateData implements Serializable {
    public String serviceName;
    public byte[] spanId;
    public byte[] parentSpanId;
    public byte[] traceId;
    public String spanKind;
    public String spanName;
    public String operation;
    public Long durationInNanos;
    public String status;
    public String endTime;
    private int error;
    private int fault;
    private String operationName;
    private String environment;
    public Map<String, String> groupByAttributes;

    public SpanStateData(final String serviceName,
                         final byte[] spanId,
                         final byte[] parentSpanId,
                         final byte[] traceId,
                         final String spanKind,
                         final String spanName,
                         final String operation,
                         final Long durationInNanos,
                         final String status,
                         final String endTime,
                         final Map<String, String> groupByAttributes,
                         final Map<String, Object> spanAttributes) {
        this.serviceName = serviceName;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.traceId = traceId;
        this.spanKind = spanKind;
        this.spanName = spanName;
        this.operation = operation;
        this.durationInNanos = durationInNanos;
        this.status = status;
        this.endTime = endTime;
        this.groupByAttributes = groupByAttributes != null ? groupByAttributes : Collections.emptyMap();
        
        computeErrorAndFault(status, spanAttributes);
        
        this.operationName = computeOperationName(spanName, spanAttributes);
        
        this.environment = computeEnvironment(spanAttributes);
    }
    
    /**
     * Compute error and fault indicators based on span status and HTTP status codes
     * 
     * @param spanStatus The span status (e.g., "ERROR", "OK", "2", etc.)
     * @param spanAttributes The span attributes containing HTTP status codes
     */
    private void computeErrorAndFault(final String spanStatus, final Map<String, Object> spanAttributes) {

        this.error = 0;
        this.fault = 0;

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
        

        final boolean hasStatus = isSpanStatusError(spanStatus);
        final boolean hasHttpStatus = (httpStatusCode != null);
        
        if (!hasStatus && !hasHttpStatus) {

            this.error = 0;
            this.fault = 0;
        } else if (!hasHttpStatus && hasStatus) {

            this.fault = 1;
            this.error = 0;
        } else if (hasHttpStatus) {

            if (httpStatusCode >= 500 && httpStatusCode <= 599) {

                this.fault = 1;
                this.error = 0;
            } else if (httpStatusCode >= 400 && httpStatusCode <= 499) {

                this.fault = 0;
                this.error = 1;
            } else {

                this.fault = 0;
                this.error = 0;
            }
        }
    }
    
    /**
     * Parse HTTP status code from various object types
     * 
     * @param statusCodeObject The status code object (Integer, String, etc.)
     * @return Parsed integer status code, or null if invalid
     */
    private Integer parseHttpStatusCode(final Object statusCodeObject) {
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
     * 
     * @param spanStatus The span status string
     * @return true if status indicates error
     */
    private boolean isSpanStatusError(final String spanStatus) {
        if (spanStatus == null) {
            return false;
        }
        


        return "ERROR".equalsIgnoreCase(spanStatus) || 
               "2".equals(spanStatus) ||
               spanStatus.toLowerCase().contains("error");
    }
    
    /**
     * Get error indicator
     * 
     * @return 1 if span has error, 0 otherwise
     */
    public int getError() {
        return error;
    }
    
    /**
     * Get fault indicator
     * 
     * @return 1 if span has fault, 0 otherwise
     */
    public int getFault() {
        return fault;
    }
    
    /**
     * Get computed operation name
     * 
     * @return Operation name derived using HTTP-aware rules
     */
    public String getOperationName() {
        return operationName;
    }
    
    /**
     * Get computed environment
     * 
     * @return Environment derived from resource attributes
     */
    public String getEnvironment() {
        return environment;
    }
    
    /**
     * Get span ID in hexadecimal string format for use with ephemeral decorations
     * 
     * @return Span ID as hex string
     */
    public String getSpanIdHex() {
        return Hex.encodeHexString(spanId);
    }
    
    /**
     * Compute operation name using HTTP-aware derivation rules
     * 
     * @param spanName The span name from the span
     * @param spanAttributes The span attributes containing HTTP method and URL information
     * @return Computed operation name
     */
    private String computeOperationName(final String spanName, final Map<String, Object> spanAttributes) {

        final String method1 = getStringAttribute(spanAttributes, "http.request.method");
        final String method2 = getStringAttribute(spanAttributes, "http.method");
        

        final boolean useHttpDerivation = spanName == null || 
                                         "UnknownOperation".equals(spanName) || 
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
     * 
     * @param path The URL path
     * @return First section of the path (e.g., "/payment/1234" -> "/payment")
     */
    private String extractFirstPathSection(final String path) {
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
     * 
     * @param spanAttributes The span attributes containing resource information
     * @return Computed environment string
     */
    private String computeEnvironment(final Map<String, Object> spanAttributes) {
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
     * 
     * @param attributes The span attributes map
     * @param key The attribute key
     * @return String value or null if not present/not a string
     */
    private String getStringAttribute(final Map<String, Object> attributes, final String key) {
        if (attributes == null) {
            return null;
        }
        
        final Object value = attributes.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Get string attribute from a map safely
     * 
     * @param map The map to get value from
     * @param key The attribute key
     * @return String value or null if not present/not a string
     */
    private String getStringAttributeFromMap(final Map<String, Object> map, final String key) {
        if (map == null) {
            return null;
        }
        
        final Object value = map.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Check if string is non-empty
     * 
     * @param value The string value to check
     * @return true if string is non-null and non-empty
     */
    private boolean isNonEmptyString(final String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpanStateData that = (SpanStateData) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Arrays.equals(spanId, that.spanId) &&
                Arrays.equals(parentSpanId, that.parentSpanId) &&
                Arrays.equals(traceId, that.traceId) &&
                Objects.equals(spanKind, that.spanKind) &&
                Objects.equals(spanName, that.spanName) &&
                Objects.equals(operation, that.operation);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, spanKind, spanName, operation);
        result = 31 * result + Arrays.hashCode(spanId);
        result = 31 * result + Arrays.hashCode(parentSpanId);
        result = 31 * result + Arrays.hashCode(traceId);
        return result;
    }
}
