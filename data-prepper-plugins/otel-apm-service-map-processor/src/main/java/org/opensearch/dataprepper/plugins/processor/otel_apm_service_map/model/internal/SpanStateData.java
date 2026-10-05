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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil.UNKNOWN_REMOTE_OPERATION;
import static org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil.UNKNOWN_REMOTE_SERVICE;

@Getter
public class SpanStateData implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SpanStateData.class);

    /** Node type of an instrumented service (one that emits SERVER spans). */
    public static final String NODE_TYPE_SERVICE = "service";
    /** Node type of a synthesized database dependency. */
    public static final String NODE_TYPE_DATABASE = "database";
    /** Node type of a synthesized message broker dependency. */
    public static final String NODE_TYPE_MESSAGING = "messaging";
    /** Node type of a synthesized external (HTTP/RPC/peer) dependency. */
    public static final String NODE_TYPE_EXTERNAL = "external";

    private static final Set<String> DEPENDENCY_CANDIDATE_KINDS = Set.of(
            "CLIENT", "SPAN_KIND_CLIENT", "PRODUCER", "SPAN_KIND_PRODUCER", "CONSUMER", "SPAN_KIND_CONSUMER");
    /** Loose dotted quad (also zero-padded forms such as 010.000.000.005, which InetAddresses rejects). */
    private static final Pattern IPV4_PATTERN = Pattern.compile("^\\d{1,3}(\\.\\d{1,3}){3}$");
    /** A numeric port, or -1, which URL.getPort() yields for schemes without a default port. */
    private static final Pattern PORT_PATTERN = Pattern.compile("^(\\d{1,5}|-1)$");
    private static final Set<String> DROPPED_PORTS = Set.of("80", "443", "-1");
    private static final Set<String> LOOPBACK_NAMES = Set.of(
            "localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback");
    private static final String AWS_API_RPC_SYSTEM = "aws-api";
    private static final String[] DB_SYSTEM_KEYS = {
            "db.system.name", "db.system", "db_system", "db_system_name", "db.system_name"};
    private static final String[] HOST_KEYS = {"server.address", "net.peer.name", "network.peer.address"};
    private static final String[] PORT_KEYS = {"server.port", "net.peer.port", "network.peer.port"};
    /** Destination-name keys; messaging.destination is the pre-1.17 semantic-convention key. */
    private static final String[] MESSAGING_DESTINATION_KEYS = {"messaging.destination.name", "messaging.destination"};

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

    /** Name of the dependency this span calls when it is not a traced service, or null. */
    private String derivedRemoteService;
    /** Operation invoked on the derived dependency, or null when it cannot be derived. */
    private String derivedRemoteOperation;
    /** Dependency node type (database, messaging or external), or null when the span targets a traced service. */
    private String derivedNodeType;
    /** The messaging.system attribute, used to synthesize broker nodes. */
    private String messagingSystem;
    /** The messaging destination name, used to synthesize broker nodes. */
    private String messagingDestination;
    /** The messaging operation (for example publish, receive or process). */
    private String messagingOperation;
    /**
     * Canonical (OTel-keyed) identity attributes for the synthesized dependency node, so consumers
     * can filter its spans/logs on exact peer identity. Empty for spans targeting a traced service.
     */
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
        this(serviceName, spanId, parentSpanId, traceId, spanKind, spanName, operation, durationInNanos, status,
                endTime, groupByAttributes, spanAttributes, DependencyNamingPolicy.DEFAULT);
    }

    /**
     * @param namingPolicy Policy for naming synthesized dependency targets, or null to skip dependency
     *                     derivation entirely (dependency nodes disabled)
     */
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
                         final Map<String, Object> spanAttributes,
                         final DependencyNamingPolicy namingPolicy) {
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
        if (namingPolicy != null && isDependencyCandidateKind(spanKind)
                && spanAttributes != null && !spanAttributes.isEmpty()) {
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
                LOG.debug("Could not derive the remote dependency of span {} in trace {}: {}", spanId, traceId, e.getMessage());
                this.derivedRemoteService = null;
                this.derivedRemoteOperation = null;
            }
            // Databases are named from the system plus the instance so that distinct databases of one
            // engine stay distinct nodes. The shared extractor only knows dotted db.system keys and names
            // a database by its system alone (or host:port when only flattened keys are present), so the
            // name is rebuilt here unless the caller named the peer (peer.service) or it is an AWS SDK call.
            if (NODE_TYPE_DATABASE.equals(derivedNodeType)
                    && !hasAny(spanAttributes, "peer.service")
                    && !AWS_API_RPC_SYSTEM.equals(stringAttr(spanAttributes, "rpc.system"))) {
                this.derivedRemoteService = computeDatabaseName(spanAttributes, namingPolicy);
            } else if (NODE_TYPE_EXTERNAL.equals(derivedNodeType) && derivedRemoteService != null) {
                this.derivedRemoteService = normalizeExternalName(derivedRemoteService, namingPolicy);
            }
            this.messagingSystem = stringAttr(spanAttributes, "messaging.system");
            this.messagingDestination = firstAttr(spanAttributes, MESSAGING_DESTINATION_KEYS);
            this.messagingOperation = firstAttr(spanAttributes, "messaging.operation", "messaging.operation.type");
            // Name a CLIENT-kind messaging call (e.g. settle/ack) like the PRODUCER/CONSUMER broker node.
            // A messaging call without a destination (metadata fetch, ack) names no broker node, as on the
            // PRODUCER/CONSUMER path.
            if (NODE_TYPE_MESSAGING.equals(derivedNodeType)) {
                this.derivedRemoteService = messagingSystem != null && messagingDestination != null
                        ? messagingSystem + ":" + messagingDestination : null;
            }

            this.dependencyAttributes = computeDependencyAttributes(derivedNodeType, spanAttributes, namingPolicy);
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
     * @param namingPolicy   The host naming policy
     * @return An ordered map of canonical identity attributes (possibly empty)
     */
    private static Map<String, String> computeDependencyAttributes(final String nodeType,
                                                                    final Map<String, Object> spanAttributes,
                                                                    final DependencyNamingPolicy namingPolicy) {
        if (nodeType == null || spanAttributes == null || spanAttributes.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, String> attrs = new LinkedHashMap<>();
        // A host that cannot name a node (IP literal, loopback, denylisted) is per-instance noise; it is
        // left out rather than stored as whichever instance's address was seen first.
        final String rawHost = firstAttr(spanAttributes, HOST_KEYS);
        final String host = rawHost != null && isNameableHost(rawHost, namingPolicy) ? rawHost : null;
        final String port = host != null ? firstAttr(spanAttributes, PORT_KEYS) : null;
        if (NODE_TYPE_DATABASE.equals(nodeType)) {
            putIfPresent(attrs, "db.system.name", firstAttr(spanAttributes, DB_SYSTEM_KEYS));
            putIfPresent(attrs, "db.namespace", firstAttr(spanAttributes, "db.namespace", "db.name"));
            putIfPresent(attrs, "server.address", host);
            putIfPresent(attrs, "server.port", port);
        } else if (NODE_TYPE_MESSAGING.equals(nodeType)) {
            // messaging.operation (publish/receive) is per-direction, not identity — it would split
            // the shared broker node between producer and consumer, so it is deliberately omitted.
            putIfPresent(attrs, "messaging.system", stringAttr(spanAttributes, "messaging.system"));
            putIfPresent(attrs, "messaging.destination.name", firstAttr(spanAttributes, MESSAGING_DESTINATION_KEYS));
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
        if (hasAny(spanAttributes, DB_SYSTEM_KEYS)
                || hasAny(spanAttributes, "db.statement", "db.query.text", "db.name", "db.namespace")) {
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
     * Build a database dependency name: {@code {system}:{host}}, else {@code {system}:{namespace}},
     * else {@code {system}}. Without a system, the host[:port] or namespace is used, falling back to
     * "database" when only a query/statement is present. IP-literal, loopback and denied hosts are
     * skipped so they never mint a node per address.
     *
     * @param attrs        The span attributes
     * @param namingPolicy The host naming policy
     * @return A database node name
     */
    private static String computeDatabaseName(final Map<String, Object> attrs, final DependencyNamingPolicy namingPolicy) {
        final String system = firstAttr(attrs, DB_SYSTEM_KEYS);
        final String rawHost = firstAttr(attrs, HOST_KEYS);
        final String host = rawHost != null && isNameableHost(rawHost, namingPolicy) ? rawHost : null;
        final String namespace = firstAttr(attrs, "db.namespace", "db.name");
        if (system != null) {
            if (host != null) {
                return system + ":" + host;
            }
            return namespace != null ? system + ":" + namespace : system;
        }
        if (host != null) {
            final String port = firstAttr(attrs, PORT_KEYS);
            return port != null ? host + ":" + port : host;
        }
        // A statement/query was present (which is why we reached here) but no identity.
        return namespace != null ? namespace : "database";
    }

    /**
     * Drop a scheme-default port (80/443) or the unknown-port marker (-1) so a peer reached with and without an explicit default port
     * is one node, and suppress hosts that are loopback names or match the denylist.
     *
     * @return The normalized name, or null when the host must not name a node
     */
    private static String normalizeExternalName(final String name, final DependencyNamingPolicy namingPolicy) {
        final String host = hostWithoutPort(name);
        if (isLoopbackName(host) || namingPolicy.isDeniedHost(host)) {
            return null;
        }
        // host:-1 is how the shared extractor renders a URL scheme without a default port.
        final String port = host.length() < name.length() ? name.substring(host.length() + 1) : null;
        if (port != null && DROPPED_PORTS.contains(port) && !host.contains(":")) {
            return host;
        }
        return name;
    }

    private static boolean isNameableHost(final String host, final DependencyNamingPolicy namingPolicy) {
        return !host.startsWith("[") && !isIpLiteral(host) && !isLoopbackName(host) && !namingPolicy.isDeniedHost(host);
    }

    private static boolean isLoopbackName(final String host) {
        final String lower = host.toLowerCase(Locale.ROOT);
        return LOOPBACK_NAMES.contains(lower) || lower.endsWith(".localhost");
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
        // "kafka:orders" or an SNS topic ARN are not IP literals and are kept. Loopback names
        // ("localhost:3500") point at a sidecar or local proxy, not a dependency.
        final String host = hostWithoutPort(name);
        return !(isLoopbackName(host) || (host.length() < name.length() && isIpLiteral(host)));
    }

    /**
     * @param name A dependency name such as {@code host:port}
     * @return The name without a trailing numeric (or {@code -1}) port
     */
    public static String hostWithoutPort(final String name) {
        final int lastColon = name.lastIndexOf(':');
        return lastColon > 0 && PORT_PATTERN.matcher(name.substring(lastColon + 1)).matches()
                ? name.substring(0, lastColon) : name;
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
