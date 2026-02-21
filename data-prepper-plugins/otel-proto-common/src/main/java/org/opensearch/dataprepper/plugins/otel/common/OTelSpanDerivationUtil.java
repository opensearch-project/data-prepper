/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.model.metric.JacksonMetric.ATTRIBUTES_KEY;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;

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
    public static final String DERIVED_REMOTE_SERVICE_ATTRIBUTE = "derived.remote_service";
    private static final Logger LOG = LoggerFactory.getLogger(OTelSpanDerivationUtil.class);
    private static final String SERVICE_MAPPINGS_FILE = "service_mappings";
    private static final String HOST_PORT_ORDERED_LIST_FILE = "hostport_attributes_ordered_list";

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
                System.out.println("--checking-----SERVER SPAN---"+isServerSpan(span));
            if (span != null && isServerSpan(span)) {
                System.out.println("-------SERVER SPAN---");
                deriveAttributesForSpan(span);
            }
        }
    }

    private AddressPortAttributeKeys getAddressPortAttributeKeys(final String str) {
        return new AddressPortAttributeKeys(str);
    }

    public List<AddressPortAttributeKeys> getAddressPortAttributeKeysList() {
        try (
            final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(HOST_PORT_ORDERED_LIST_FILE);
        ) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines()
                .filter(line -> line.contains(","))
                .map(this::getAddressPortAttributeKeys)
                .collect(Collectors.toList());
            } catch (IOException e) {
                throw e;
            }
        } catch (final Exception e) {
            LOG.error("An exception occurred while initializing hostport attribute list for Data Prepper", e);
        }
        return null;
    }


    public Map<String, String> getAwsServiceMappingsFromResources() {
        try (
            final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(SERVICE_MAPPINGS_FILE);
        ) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines()
                .filter(line -> line.contains(","))
                .collect(Collectors.toMap(
                    line -> line.split(",", 2)[0].trim(),
                    line -> line.split(",", 2)[1].trim(),
                    (oldValue, newValue) -> newValue
                ));
            } catch (IOException e) {
                throw e;
            }
        } catch (final Exception e) {
            LOG.error("An exception occurred while initializing service mappings for Data Prepper", e);
        }
        return null;
    }

    private static boolean isServerSpan(Span span) {
        return SpanKind.SPAN_KIND_SERVER.name().equals(span.getKind());
    }

    private static boolean isClientSpan(Span span) {
        return SpanKind.SPAN_KIND_CLIENT.name().equals(span.getKind());
    }

    private static boolean isProducerSpan(Span span) {
        return SpanKind.SPAN_KIND_PRODUCER.name().equals(span.getKind());
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

            String operationName = null;
            if (isServerSpan(span)) {
                operationName = computeOperationName(span.getName(), spanAttributes);
            } else if (isClientSpan(span) || isProducerSpan(span)) {
                final RemoteOperationAndService remoteOperationAndService = computeRemoteOperationAndService(spanAttributes);
                operationName = remoteOperationAndService.getOperation();
                putAttribute(span, DERIVED_REMOTE_SERVICE_ATTRIBUTE, remoteOperationAndService.getService());
            }

            final String environment = computeEnvironment(spanAttributes);

            // Add derived attributes using our safe attribute setting method
            putAttribute(span, DERIVED_FAULT_ATTRIBUTE, String.valueOf(errorFault.fault));
            putAttribute(span, DERIVED_ERROR_ATTRIBUTE, String.valueOf(errorFault.error));
            if (operationName != null) {
                putAttribute(span, DERIVED_OPERATION_ATTRIBUTE, operationName);
            }
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

    private static String extractFirstPathFromUrl(final String url) {
        int colonDoubleSlash = url.indexOf("://");
        int firstSlash = url.indexOf("/", colonDoubleSlash+3);
        int secondSlash = url.indexOf("/", firstSlash+1);
        String result= (secondSlash > 0) ? url.substring(firstSlash, secondSlash) : url.substring(firstSlash);
        return result;
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

    public static RemoteOperationAndService computeRemoteOperationAndService(final Map<String, Object> spanAttributes) {
        OTelSpanDerivationUtil oTelSpanDerivationUtil = new OTelSpanDerivationUtil();
        Map<String, String> awsServiceMappings = oTelSpanDerivationUtil.getAwsServiceMappingsFromResources();
        RemoteOperationAndServiceProviders remoteOperationAndServiceProviders = new RemoteOperationAndServiceProviders();
        List<OTelSpanDerivationUtil.AddressPortAttributeKeys> addressPortAttributeKeysList = oTelSpanDerivationUtil.getAddressPortAttributeKeysList();

        RemoteOperationAndService remoteOperationAndService = new RemoteOperationAndService(null, null);
        if (remoteOperationAndServiceProviders.AwsRpcRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.AwsRpcRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, awsServiceMappings);
        }
        if (remoteOperationAndServiceProviders.DbRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.DbRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, null);
        }
        if (remoteOperationAndServiceProviders.DbQueryRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.DbQueryRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, null);
        }
        if (remoteOperationAndServiceProviders.FaasRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.FaasRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, null);
        }
        if (remoteOperationAndServiceProviders.MessagingSystemRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.MessagingSystemRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, null);
        }
        if (remoteOperationAndServiceProviders.GraphQlRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.GraphQlRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, null);
        }
        if (remoteOperationAndServiceProviders.AwsRpcRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.AwsRpcRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, awsServiceMappings);
        }
        if (remoteOperationAndServiceProviders.PeerServiceRemoteOperationServiceExtractor.appliesToSpan(spanAttributes)) {
            remoteOperationAndService = remoteOperationAndServiceProviders.PeerServiceRemoteOperationServiceExtractor.getRemoteOperationAndService(spanAttributes, null);
        }

        if (!remoteOperationAndService.isNull()) {
            return remoteOperationAndService;
        }

        // Fallback: derive from URL or network attributes
        final String urlString = getStringAttribute(spanAttributes, "url.full") != null
                ? getStringAttribute(spanAttributes, "url.full")
                : getStringAttribute(spanAttributes, "http.url");

        String remoteOperation = remoteOperationAndService.getOperation();
        String remoteService = remoteOperationAndService.getService();
        if (remoteService == null) {
            remoteService = deriveServiceFromNetwork(spanAttributes, urlString, addressPortAttributeKeysList);
        }

        if (remoteOperation == null && urlString != null) {
            final String httpMethod = getStringAttribute(spanAttributes, "http.request.method") != null
                    ? getStringAttribute(spanAttributes, "http.request.method")
                    : getStringAttribute(spanAttributes, "http.method");
            remoteOperation = httpMethod != null ? httpMethod + " " + extractFirstPathFromUrl(urlString) : urlString;
        }

        return new RemoteOperationAndService(
                remoteOperation != null ? remoteOperation : "UnknownRemoteOperation",
                remoteService != null ? remoteService : "UnknownRemoteService");
    }

    private static String deriveServiceFromNetwork(final Map<String, Object> spanAttributes, final String urlString, final List<AddressPortAttributeKeys> addressPortAttributeKeysList) {

        for (AddressPortAttributeKeys addressPortAttributeKeys : addressPortAttributeKeysList) {
            final String address = getStringAttribute(spanAttributes, addressPortAttributeKeys.getAddress());
            if (address != null) {
                final String port = getStringAttribute(spanAttributes, addressPortAttributeKeys.getPort());
                return port != null ? address + ":" + port : address;
            }
        }

        if (urlString != null) {
            try {
                final URL url = new URL(urlString);
                final int port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
                return url.getHost() + ":" + port;
            } catch (MalformedURLException ignored) {}
        }

        return null;
    }

    /**
     * Compute environment from resource attributes.
     * Package-private for testing purposes only.
     *
     * @param spanAttributes The span attributes containing resource information
     * @return Computed environment string
     */
    public static String computeEnvironment(final Map<String, Object> spanAttributes) {
        String env = ServiceEnvironmentProviders.getAwsServiceEnvironment(spanAttributes);
        if (env == null) {
            env = ServiceEnvironmentProviders.getDeploymentEnvironment(spanAttributes);
        }
        return env;
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


    public static class AddressPortAttributeKeys {
        final String address;
        final String port;
        public AddressPortAttributeKeys(final String str) {
            this.address = str.split(",")[0];
            this.port = str.split(",")[1];
        }

        public String getAddress() {
            return address;
        }

        public String getPort() {
            return port;
        }
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
