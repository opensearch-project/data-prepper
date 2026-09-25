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
import com.google.common.net.InetAddresses;
import lombok.Getter;

import org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil;
import org.opensearch.dataprepper.plugins.otel.common.RemoteOperationAndService;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil.UNKNOWN_REMOTE_OPERATION;
import static org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil.UNKNOWN_REMOTE_SERVICE;

@Getter
public class SpanStateData implements Serializable {
    // Node type classifications for synthesized (non-service) dependency targets.
    public static final String NODE_TYPE_SERVICE = "service";
    public static final String NODE_TYPE_DATABASE = "database";
    public static final String NODE_TYPE_MESSAGING = "messaging";
    public static final String NODE_TYPE_EXTERNAL = "external";

    private static final Set<String> DEPENDENCY_CANDIDATE_KINDS = Set.of(
            "CLIENT", "SPAN_KIND_CLIENT", "PRODUCER", "SPAN_KIND_PRODUCER", "CONSUMER", "SPAN_KIND_CONSUMER");
    // Loose dotted quad (also zero-padded forms such as 010.000.000.005, which InetAddresses rejects).
    private static final Pattern IPV4_PATTERN = Pattern.compile("^\\d{1,3}(\\.\\d{1,3}){3}$");
    // URL.getPort() yields -1 for schemes without a default port.
    private static final Pattern PORT_PATTERN = Pattern.compile("^(\\d{1,5}|-1)$");

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
    // Canonical (OTel-keyed) identity attributes for the synthesized dependency node, so consumers
    // can filter its spans/logs on exact peer identity. Empty for spans targeting a traced service.
    private Map<String, String> dependencyAttributes = Collections.emptyMap();

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

        // Derived remote-dependency identity is computed only for the span kinds that can target an
        // external dependency: CLIENT calls and messaging PRODUCER/CONSUMER spans. SERVER/INTERNAL
        // spans describe the service itself (their server.address/http.method is the service's own
        // listener), so leaving these null avoids persisting misleading data across the windows.
        if (isDependencyCandidateKind(spanKind) && spanAttributes != null && !spanAttributes.isEmpty()) {
            this.derivedNodeType = computeNodeType(spanAttributes);
            try {
                final RemoteOperationAndService remote =
                        OTelSpanDerivationUtil.computeRemoteOperationAndService(spanAttributes);
                if (remote != null) {
                    this.derivedRemoteService = remote.getService();
                    // Leave the operation unset when unresolved so callers fall back to the span's own.
                    this.derivedRemoteOperation = UNKNOWN_REMOTE_OPERATION.equals(remote.getOperation())
                            ? null : remote.getOperation();
                }
            } catch (final RuntimeException e) {
                // A malformed URL/authority can throw inside the shared extractor. A failed
                // dependency derivation must not drop the whole span (its service->service
                // contribution still counts); leave the derived fields unset instead.
                this.derivedRemoteService = null;
                this.derivedRemoteOperation = null;
            }
            // The shared extractor only recognizes dotted db keys (db.system[.name]);
            // many instrumentations emit flattened variants (db_system, db_system_name),
            // which otherwise fall through to "UnknownRemoteService". Recover a database
            // name from those variants so the dependency isn't lost.
            if (NODE_TYPE_DATABASE.equals(derivedNodeType)
                    && (derivedRemoteService == null
                        || UNKNOWN_REMOTE_SERVICE.equals(derivedRemoteService))) {
                final String dbName = computeDatabaseName(spanAttributes);
                if (dbName != null) {
                    this.derivedRemoteService = dbName;
                }
            }
            this.messagingSystem = stringAttr(spanAttributes, "messaging.system");
            this.messagingDestination = stringAttr(spanAttributes, "messaging.destination.name");
            this.messagingOperation = stringAttr(spanAttributes, "messaging.operation");
            // Name a CLIENT-kind messaging call (e.g. settle/ack) like the PRODUCER/CONSUMER broker node.
            if (NODE_TYPE_MESSAGING.equals(derivedNodeType) && messagingSystem != null && messagingDestination != null) {
                this.derivedRemoteService = messagingSystem + ":" + messagingDestination;
            }

            this.dependencyAttributes = computeDependencyAttributes(derivedNodeType, spanAttributes);
        }
    }

    /**
     * Whether a span kind can target an external dependency (client calls, messaging producers and
     * consumers). Matches both bare ({@code CLIENT}) and prefixed ({@code SPAN_KIND_CLIENT}) forms.
     */
    private static boolean isDependencyCandidateKind(final String spanKind) {
        if (spanKind == null) {
            return false;
        }
        return DEPENDENCY_CANDIDATE_KINDS.contains(spanKind);
    }

    /**
     * Collect the identity attributes for a synthesized dependency node, under canonical OTel keys,
     * so consumers can filter the dependency's spans/logs on exact peer identity rather than parsing
     * the node name. Only non-empty values are included; returns an empty map for service targets.
     *
     * @param nodeType       The derived node type (database / messaging / external), or null
     * @param spanAttributes The span attributes
     * @return An ordered map of canonical identity attributes (possibly empty)
     */
    private static Map<String, String> computeDependencyAttributes(final String nodeType,
                                                                    final Map<String, Object> spanAttributes) {
        if (nodeType == null || spanAttributes == null || spanAttributes.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, String> attrs = new LinkedHashMap<>();
        final String host = firstAttr(spanAttributes, "server.address", "net.peer.name", "network.peer.address");
        final String port = firstAttr(spanAttributes, "server.port", "net.peer.port", "network.peer.port");
        if (NODE_TYPE_DATABASE.equals(nodeType)) {
            putIfPresent(attrs, "db.system.name",
                    firstAttr(spanAttributes, "db.system.name", "db.system", "db_system", "db_system_name", "db.system_name"));
            putIfPresent(attrs, "db.namespace", firstAttr(spanAttributes, "db.namespace", "db.name"));
            putIfPresent(attrs, "server.address", host);
            putIfPresent(attrs, "server.port", port);
        } else if (NODE_TYPE_MESSAGING.equals(nodeType)) {
            // messaging.operation (publish/receive) is per-direction, not identity — it would split
            // the shared broker node between producer and consumer, so it is deliberately omitted.
            putIfPresent(attrs, "messaging.system", stringAttr(spanAttributes, "messaging.system"));
            putIfPresent(attrs, "messaging.destination.name", stringAttr(spanAttributes, "messaging.destination.name"));
        } else if (NODE_TYPE_EXTERNAL.equals(nodeType)) {
            // url.full is deliberately excluded: it carries the per-request path + query string
            // (ids, tokens, PII) and would otherwise land in the service-map index. The bounded
            // peer identity below is what distinguishes an external dependency.
            putIfPresent(attrs, "peer.service", stringAttr(spanAttributes, "peer.service"));
            putIfPresent(attrs, "server.address", host);
            putIfPresent(attrs, "server.port", port);
            putIfPresent(attrs, "rpc.system", stringAttr(spanAttributes, "rpc.system"));
        }
        return attrs.isEmpty() ? Collections.emptyMap() : attrs;
    }

    private static void putIfPresent(final Map<String, String> target, final String key, final String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
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
        if (hasAny(spanAttributes, "db.system.name", "db.system", "db_system", "db_system_name", "db.system_name",
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
     * the DB system (dotted or flattened variants) when known, otherwise the server
     * host[:port]. Falls back to "database" when only a query/statement is present.
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
     * Whether a derived dependency name is worth synthesizing a node for. Suppresses unresolved
     * ({@code UnknownRemoteService}) and raw-IP peers (which otherwise explode the map into a node
     * per address) at the source, so the topology only shows named dependencies.
     *
     * @param name The derived dependency name, e.g. {@code postgresql}, {@code host:port} or {@code kafka:orders}
     * @return true if a node should be synthesized for this name
     */
    public static boolean isPublishableDependencyName(final String name) {
        if (name == null || name.isEmpty() || UNKNOWN_REMOTE_SERVICE.equals(name)) {
            return false;
        }
        // An empty host or destination (":80", "kafka:") names nothing.
        if (name.startsWith(":") || name.endsWith(":")) {
            return false;
        }
        // A bracketed form ("[::1]", "[::1]:6379") only ever wraps an IPv6 literal.
        if (name.startsWith("[")) {
            return false;
        }
        if (isIpLiteral(name)) {
            return false;
        }
        // address + ":" + port, including an unbracketed IPv6 address. Names such as "AWS::DynamoDB",
        // "kafka:orders" or an SNS topic ARN are not IP literals and are kept.
        final int lastColon = name.lastIndexOf(':');
        return !(lastColon > 0
                && PORT_PATTERN.matcher(name.substring(lastColon + 1)).matches()
                && isIpLiteral(name.substring(0, lastColon)));
    }

    private static boolean isIpLiteral(final String host) {
        return IPV4_PATTERN.matcher(host).matches() || InetAddresses.isInetAddress(host);
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
