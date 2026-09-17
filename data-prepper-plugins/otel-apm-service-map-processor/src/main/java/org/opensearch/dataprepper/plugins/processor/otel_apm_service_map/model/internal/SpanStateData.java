/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;

import org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil;
import org.opensearch.dataprepper.plugins.otel.common.RemoteOperationAndService;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@Getter
public class SpanStateData implements Serializable {
    // Node type classifications for synthesized (non-service) dependency targets.
    public static final String NODE_TYPE_SERVICE = "service";
    public static final String NODE_TYPE_DATABASE = "database";
    public static final String NODE_TYPE_MESSAGING = "messaging";
    public static final String NODE_TYPE_EXTERNAL = "external";

    private String serviceName;
    private String spanId;
    private String parentSpanId;
    private String traceId;
    private String spanKind;
    private String spanName;
    private String operation;
    private Long durationInNanos;
    private String status;
    private String endTime;
    private int error;
    private int fault;
    private String operationName;
    private String environment;
    private Map<String, String> groupByAttributes;

    // Derived remote-dependency fields for spans whose downstream is not a traced service
    // (databases, message brokers, external endpoints). Computed once from span attributes.
    private String derivedRemoteService;
    private String derivedRemoteOperation;
    private String derivedNodeType;
    // Messaging (PRODUCER/CONSUMER) attributes for broker-node synthesis.
    private String messagingSystem;
    private String messagingDestination;
    private String messagingOperation;

    public SpanStateData(final String serviceName,
                         final String spanId,
                         final String parentSpanId,
                         final String traceId,
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

        OTelSpanDerivationUtil.ErrorFaultResult errorFault = OTelSpanDerivationUtil.computeErrorAndFault(status, spanAttributes);
        this.error = errorFault.getError();
        this.fault = errorFault.getFault();

        this.operationName = OTelSpanDerivationUtil.computeOperationName(spanName, spanAttributes);

        this.environment = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        // Derive remote-dependency identity (database / external / messaging) from span attributes.
        // The service-map processor uses these to synthesize typed target nodes for CLIENT spans that
        // have no downstream SERVER span, and for PRODUCER/CONSUMER spans that reference a broker.
        this.derivedNodeType = computeNodeType(spanAttributes);

        if (spanAttributes != null && !spanAttributes.isEmpty()) {
            final RemoteOperationAndService remote =
                    OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);
            if (remote != null) {
                this.derivedRemoteService = remote.getService();
                this.derivedRemoteOperation = remote.getOperation();
            }
            // The shared extractor only recognizes dotted db keys (db.system[.name]);
            // many instrumentations emit flattened variants (db_system, db_system_name),
            // which otherwise fall through to "UnknownRemoteService". Recover a database
            // name from those variants so the dependency isn't lost.
            if (NODE_TYPE_DATABASE.equals(derivedNodeType)
                    && (derivedRemoteService == null
                        || "UnknownRemoteService".equals(derivedRemoteService))) {
                final String dbName = computeDatabaseName(spanAttributes);
                if (dbName != null) {
                    this.derivedRemoteService = dbName;
                }
            }
            this.messagingSystem = stringAttr(spanAttributes, "messaging.system");
            this.messagingDestination = stringAttr(spanAttributes, "messaging.destination.name");
            this.messagingOperation = stringAttr(spanAttributes, "messaging.operation");
        }
    }

    /**
     * Classify the downstream dependency type of a span from its attributes, following OTel
     * semantic conventions. Returns {@link #NODE_TYPE_MESSAGING}, {@link #NODE_TYPE_DATABASE},
     * {@link #NODE_TYPE_EXTERNAL}, or {@code null} when the span targets a normal traced service.
     *
     * @param spanAttributes The span attributes (may be null)
     * @return The derived node type, or null when not an external dependency
     */
    private static String computeNodeType(final Map<String, Object> spanAttributes) {
        if (spanAttributes == null || spanAttributes.isEmpty()) {
            return null;
        }
        if (hasAny(spanAttributes, "messaging.system")) {
            return NODE_TYPE_MESSAGING;
        }
        // Database detection covers current and legacy OTel keys plus flattened variants
        // (db_system, db_system_name) emitted by several instrumentations.
        if (hasAny(spanAttributes, "db.system.name", "db.system", "db_system", "db_system_name",
                "db.statement", "db.query.text", "db.name", "db.namespace")) {
            return NODE_TYPE_DATABASE;
        }
        // External HTTP / RPC / generic peer endpoints.
        if (hasAny(spanAttributes, "url.full", "http.url", "http.request.method", "http.method",
                "peer.service", "rpc.system", "server.address", "net.peer.name")) {
            return NODE_TYPE_EXTERNAL;
        }
        return null;
    }

    private static boolean hasAny(final Map<String, Object> attrs, final String... keys) {
        for (final String key : keys) {
            if (attrs.get(key) != null) {
                return true;
            }
        }
        return false;
    }

    private static String stringAttr(final Map<String, Object> attrs, final String key) {
        final Object value = attrs.get(key);
        return value != null ? value.toString() : null;
    }

    private static String firstAttr(final Map<String, Object> attrs, final String... keys) {
        for (final String key : keys) {
            final Object value = attrs.get(key);
            if (value != null && !value.toString().isEmpty()) {
                return value.toString();
            }
        }
        return null;
    }

    /**
     * Build a database dependency name from whatever identity attributes are present:
     * the DB system (dotted or flattened variants), optionally suffixed with the
     * server host:port. Falls back to "database" when only a query/statement is present.
     *
     * @param attrs The span attributes
     * @return A database node name, or null if nothing identifying is present
     */
    private static String computeDatabaseName(final Map<String, Object> attrs) {
        final String system = firstAttr(attrs, "db.system.name", "db.system", "db_system",
                "db_system_name", "db.system_name");
        final String host = firstAttr(attrs, "server.address", "net.peer.name", "network.peer.address");
        final String port = firstAttr(attrs, "server.port", "net.peer.port", "network.peer.port");
        if (system != null) {
            return system;
        }
        if (host != null) {
            return port != null ? host + ":" + port : host;
        }
        // A statement/query was present (which is why we reached here) but no identity.
        return "database";
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpanStateData that = (SpanStateData) o;
        return Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(spanId, that.spanId) &&
                Objects.equals(traceId, that.traceId) &&
                Objects.equals(parentSpanId, that.parentSpanId) &&
                Objects.equals(spanKind, that.spanKind) &&
                Objects.equals(spanName, that.spanName) &&
                Objects.equals(operation, that.operation);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, spanKind, spanName, operation);
        result = 31 * result + ((spanId == null) ? 0 : spanId.hashCode());
        result = 31 * result + ((parentSpanId == null) ? 0 : parentSpanId.hashCode());
        result = 31 * result + ((traceId == null) ? 0 : traceId.hashCode());
        return result;
    }

    @VisibleForTesting
    public void setDurationInNanos(Long duration) {
        durationInNanos = duration;
    }

    @VisibleForTesting
    public void setStatus(String status) {
        this.status = status;
    }
}
