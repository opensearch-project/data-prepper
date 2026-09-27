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

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.SpanStateData.isPublishableDependencyName;

class SpanStateDataTest {

    private static SpanStateData spanWithAttributes(final String spanKind,
                                                    final Map<String, Object> attributes) {
        return new SpanStateData(
            "service", Hex.encodeHexString(new byte[]{1}), null, Hex.encodeHexString(new byte[]{2}), spanKind,
            "span", "op", 1000L, "OK", "2023-01-01", null, attributes
        );
    }

    @Test
    void dependencyAttributes_forDatabase_capturesCanonicalIdentity() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("db.system.name", "postgresql");
        attributes.put("db.namespace", "orders");
        attributes.put("server.address", "db-host");
        attributes.put("server.port", 5432);

        Map<String, String> deps = spanWithAttributes("SPAN_KIND_CLIENT", attributes).getDependencyAttributes();

        assertEquals("postgresql", deps.get("db.system.name"));
        assertEquals("orders", deps.get("db.namespace"));
        assertEquals("db-host", deps.get("server.address"));
        assertEquals("5432", deps.get("server.port"));
    }

    @Test
    void dependencyAttributes_forDatabase_recoversFlattenedSystemKey() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("db_system_name", "mysql");
        attributes.put("db.statement", "SELECT 1");

        Map<String, String> deps = spanWithAttributes("SPAN_KIND_CLIENT", attributes).getDependencyAttributes();

        assertEquals("mysql", deps.get("db.system.name"));
    }

    @Test
    void dependencyAttributes_forMessaging_capturesSystemAndDestination() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("messaging.system", "kafka");
        attributes.put("messaging.destination.name", "orders");
        attributes.put("messaging.operation", "publish");

        Map<String, String> deps = spanWithAttributes("SPAN_KIND_PRODUCER", attributes).getDependencyAttributes();

        assertEquals("kafka", deps.get("messaging.system"));
        assertEquals("orders", deps.get("messaging.destination.name"));
        // messaging.operation is per-direction, not identity — it must NOT be in the broker's attrs.
        assertNull(deps.get("messaging.operation"));
    }

    @Test
    void dependencyAttributes_forExternal_capturesPeerIdentity() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("server.address", "api.openai.com");
        attributes.put("server.port", 443);

        Map<String, String> deps = spanWithAttributes("SPAN_KIND_CLIENT", attributes).getDependencyAttributes();

        assertEquals("api.openai.com", deps.get("server.address"));
        assertEquals("443", deps.get("server.port"));
    }

    @Test
    void dependencyAttributes_forService_isEmpty() {
        assertTrue(spanWithAttributes("SPAN_KIND_SERVER", null).getDependencyAttributes().isEmpty());
    }

    @Test
    void derivedNodeType_classifiesMessagingFirst() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("messaging.system", "kafka");
        // db + http attributes present too — messaging must win.
        attributes.put("db.system.name", "postgresql");
        attributes.put("http.request.method", "POST");

        assertEquals("messaging", spanWithAttributes("SPAN_KIND_PRODUCER", attributes).getDerivedNodeType());
    }

    @Test
    void derivedNodeType_classifiesDatabase() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("db.system.name", "postgresql");

        assertEquals("database", spanWithAttributes("SPAN_KIND_CLIENT", attributes).getDerivedNodeType());
    }

    @Test
    void derivedNodeType_classifiesExternal() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("server.address", "api.openai.com");

        assertEquals("external", spanWithAttributes("SPAN_KIND_CLIENT", attributes).getDerivedNodeType());
    }

    @Test
    void derivedNodeType_derivesOnlyForExactDependencyKinds() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("server.address", "api.openai.com");

        for (final String kind : new String[]{"CLIENT", "SPAN_KIND_CLIENT", "PRODUCER", "SPAN_KIND_PRODUCER",
                "CONSUMER", "SPAN_KIND_CONSUMER"}) {
            assertThat(kind, spanWithAttributes(kind, attributes).getDerivedNodeType(), equalTo("external"));
        }
        // Substring matches of a dependency kind are not dependency kinds.
        for (final String kind : new String[]{"SPAN_KIND_CLIENT_STREAMING", "NOT_A_PRODUCER", "client"}) {
            assertThat(kind, spanWithAttributes(kind, attributes).getDerivedNodeType(), nullValue());
        }
    }

    @Test
    void derivedNodeType_classifiesDatabaseFromUnderscoredSystemNameKey() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("db.system_name", "mysql");
        attributes.put("server.address", "orders-db");

        final SpanStateData data = spanWithAttributes("SPAN_KIND_CLIENT", attributes);

        assertThat(data.getDerivedNodeType(), equalTo("database"));
        assertThat(data.getDependencyAttributes().get("db.system.name"), equalTo("mysql"));
    }

    @Test
    void derivedRemoteService_forClientMessagingSpan_matchesBrokerName() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("messaging.system", "kafka");
        attributes.put("messaging.destination.name", "orders");
        attributes.put("messaging.operation", "settle");

        final SpanStateData data = spanWithAttributes("SPAN_KIND_CLIENT", attributes);

        assertThat(data.getDerivedNodeType(), equalTo("messaging"));
        assertThat(data.getDerivedRemoteService(), equalTo("kafka:orders"));
    }

    @Test
    void derivedRemoteOperation_isNullWhenExtractorCannotResolveOne() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("peer.service", "stripe");

        final SpanStateData data = spanWithAttributes("SPAN_KIND_CLIENT", attributes);

        assertThat(data.getDerivedRemoteService(), equalTo("stripe"));
        assertThat(data.getDerivedRemoteOperation(), nullValue());
    }

    @Test
    void derivedNodeType_isNullForPlainService() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("thread.id", 7);

        assertNull(spanWithAttributes("SPAN_KIND_SERVER", attributes).getDerivedNodeType());
    }

    @Test
    void isPublishableDependencyName_filtersUnresolvedAndRawIpPeers() {
        // Named dependencies are published.
        assertThat(isPublishableDependencyName("postgresql"), equalTo(true));
        assertThat(isPublishableDependencyName("postgresql:5432"), equalTo(true));
        assertThat(isPublishableDependencyName("api.openai.com:443"), equalTo(true));
        // Unresolved / empty are suppressed.
        assertThat(isPublishableDependencyName(null), equalTo(false));
        assertThat(isPublishableDependencyName(""), equalTo(false));
        assertThat(isPublishableDependencyName("UnknownRemoteService"), equalTo(false));
        // An empty host or destination on either side of the colon names nothing.
        assertThat(isPublishableDependencyName(":80"), equalTo(false));
        assertThat(isPublishableDependencyName("kafka:"), equalTo(false));
        assertThat(isPublishableDependencyName(":orders"), equalTo(false));
        // Raw-IP peers are redacted.
        assertThat(isPublishableDependencyName("10.0.0.5"), equalTo(false));
        assertThat(isPublishableDependencyName("10.0.0.5:5432"), equalTo(false));
        // URL schemes without a default port yield ":-1" from URL.getPort().
        assertThat(isPublishableDependencyName("10.0.0.5:-1"), equalTo(false));
        assertThat(isPublishableDependencyName("[::1]:6379"), equalTo(false));
    }

    @Test
    void isPublishableDependencyName_publishesMultiColonNamesThatAreNotIpv6Literals() {
        // AWS SDK targets and ARN-shaped messaging destinations carry several colons but are names.
        assertThat(isPublishableDependencyName("AWS::DynamoDB"), equalTo(true));
        assertThat(isPublishableDependencyName(
                "aws_sns:arn:aws:sns:us-east-1:123456789012:orders"), equalTo(true));
        assertThat(isPublishableDependencyName(
                "aws_sqs:https://sqs.us-east-1.amazonaws.com/123456789012/orders"), equalTo(true));
        // IPv6 literals in every form are still suppressed.
        assertThat(isPublishableDependencyName("::1"), equalTo(false));
        assertThat(isPublishableDependencyName("2001:db8::1"), equalTo(false));
        assertThat(isPublishableDependencyName("fe80::1%eth0"), equalTo(false));
        assertThat(isPublishableDependencyName("::ffff:10.0.0.5"), equalTo(false));
        assertThat(isPublishableDependencyName("[2001:db8::1]"), equalTo(false));
        assertThat(isPublishableDependencyName("[2001:db8::1]:6379"), equalTo(false));
        // Unbracketed IPv6 with an appended port, as deriveServiceFromNetwork builds address + ":" + port.
        assertThat(isPublishableDependencyName("2600:1f18:61c:c901:3d4f:8e2a:9b1c:7d05:443"), equalTo(false));
        assertThat(isPublishableDependencyName("::ffff:10.0.0.5:5432"), equalTo(false));
        // Zero-padded dotted quads are still raw IPv4 peers.
        assertThat(isPublishableDependencyName("010.000.000.005:5432"), equalTo(false));
        // Any bracketed form is an IPv6 literal (or malformed); never publish it.
        assertThat(isPublishableDependencyName("[]"), equalTo(false));
        assertThat(isPublishableDependencyName("[]:443"), equalTo(false));
        assertThat(isPublishableDependencyName("[foo"), equalTo(false));
    }

    @Test
    void constructor_withValidData_createsInstance() {
        byte[] spanId = {1, 2, 3};
        byte[] traceId = {4, 5, 6};
        
        SpanStateData data = new SpanStateData(
            "test-service", Hex.encodeHexString(spanId), null, Hex.encodeHexString(traceId), "CLIENT", 
            "test-span", "test-op", 1000L, "OK", "2023-01-01", 
            null, null
        );
        
        assertEquals("test-service", data.getServiceName());
        assertEquals(Hex.encodeHexString(spanId), data.getSpanId());
        assertEquals("CLIENT", data.getSpanKind());
    }

    @Test
    void getError_withErrorStatus_returnsOne() {
        SpanStateData data = new SpanStateData(
            "service", Hex.encodeHexString(new byte[]{1}), null, Hex.encodeHexString(new byte[]{2}), "CLIENT",
            "span", "op", 1000L, "ERROR", "2023-01-01", null, null
        );
        
        assertEquals(1, data.getFault());
        assertEquals(0, data.getError());
    }

    @Test
    void getError_withHttpClientError_returnsOne() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("http.response.status_code", 404);
        
        SpanStateData data = new SpanStateData(
            "service", Hex.encodeHexString(new byte[]{1}), null, Hex.encodeHexString(new byte[]{2}), "CLIENT",
            "span", "op", 1000L, "OK", "2023-01-01", null, attributes
        );
        
        assertEquals(0, data.getFault());
        assertEquals(1, data.getError());
    }

    @Test
    void getOperationName_withHttpMethod_returnsFormattedName() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("http.request.method", "GET");
        attributes.put("http.path", "/api/users");
        
        SpanStateData data = new SpanStateData(
            "service", Hex.encodeHexString(new byte[]{1}), null, Hex.encodeHexString(new byte[]{2}), "CLIENT",
            "GET", "op", 1000L, "OK", "2023-01-01", null, attributes
        );
        
        assertEquals("GET /api", data.getOperationName());
    }

    @Test
    void getEnvironment_withDeploymentEnvironment_returnsEnvironment() {
        Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("deployment.environment.name", "production");
        
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttrs);
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("resource", resource);
        
        SpanStateData data = new SpanStateData(
            "service", Hex.encodeHexString(new byte[]{1}), null, Hex.encodeHexString(new byte[]{2}), "CLIENT",
            "span", "op", 1000L, "OK", "2023-01-01", null, attributes
        );
        
        assertEquals("production", data.getEnvironment());
    }

    @Test
    void equals_withSameData_returnsTrue() {
        byte[] spanId = {1, 2, 3};
        byte[] traceId = {4, 5, 6};
        
        SpanStateData data1 = new SpanStateData(
            "service", Hex.encodeHexString(spanId), null, Hex.encodeHexString(traceId), "CLIENT",
            "span", "op", 1000L, "OK", "2023-01-01", null, null
        );
        
        SpanStateData data2 = new SpanStateData(
            "service", Hex.encodeHexString(spanId), null, Hex.encodeHexString(traceId), "CLIENT",
            "span", "op", 1000L, "OK", "2023-01-01", null, null
        );
        
        assertEquals(data1, data2);
        assertEquals(data1.hashCode(), data2.hashCode());
    }

    // ---- Review round 2: database identity, host filtering, port normalization, messaging keys ----

    private static String derivedName(final String spanKind, final Map<String, Object> attributes) {
        return spanWithAttributes(spanKind, attributes).getDerivedRemoteService();
    }

    @Test
    void derivedRemoteService_forDatabase_isSystemAndHost() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("db.system.name", "postgresql");
        attributes.put("db.namespace", "orders");
        attributes.put("server.address", "orders-db.internal");
        attributes.put("server.port", 5432);

        final SpanStateData span = spanWithAttributes("SPAN_KIND_CLIENT", attributes);

        assertThat(span.getDerivedRemoteService(), equalTo("postgresql:orders-db.internal"));
        assertThat(span.getDependencyAttributes().get("db.system.name"), equalTo("postgresql"));
    }

    @Test
    void derivedRemoteService_forDatabaseWithoutHost_isSystemAndNamespace() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("db.system", "postgresql");
        attributes.put("db.name", "billing");

        assertThat(derivedName("SPAN_KIND_CLIENT", attributes), equalTo("postgresql:billing"));
    }

    @Test
    void derivedRemoteService_forDatabaseWithOnlySystem_isSystem() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("db.system.name", "redis");

        assertThat(derivedName("SPAN_KIND_CLIENT", attributes), equalTo("redis"));
    }

    @Test
    void derivedRemoteService_forDatabaseWithIpOrDeniedHost_fallsBackToNamespaceOrSystem() {
        final Map<String, Object> ipHost = new HashMap<>();
        ipHost.put("db.system.name", "postgresql");
        ipHost.put("db.namespace", "orders");
        ipHost.put("server.address", "10.0.0.5");
        final Map<String, Object> deniedHost = new HashMap<>();
        deniedHost.put("db.system.name", "mysql");
        deniedHost.put("server.address", "ip-10-0-0-5.ec2.internal");
        final Map<String, Object> loopbackHost = new HashMap<>();
        loopbackHost.put("db.system.name", "redis");
        loopbackHost.put("server.address", "localhost");

        assertThat(derivedName("SPAN_KIND_CLIENT", ipHost), equalTo("postgresql:orders"));
        assertThat(derivedName("SPAN_KIND_CLIENT", deniedHost), equalTo("mysql"));
        assertThat(derivedName("SPAN_KIND_CLIENT", loopbackHost), equalTo("redis"));
    }

    @Test
    void derivedRemoteService_forFlattenedDbSystemWithNetworkAddress_usesSystemNaming() {
        final Map<String, Object> ipAddress = new HashMap<>();
        ipAddress.put("db_system", "postgresql");
        ipAddress.put("server.address", "10.0.0.5");
        ipAddress.put("server.port", 5432);
        final Map<String, Object> flattened = new HashMap<>();
        flattened.put("db_system", "postgresql");
        flattened.put("server.address", "pg-host");
        flattened.put("server.port", 5432);
        final Map<String, Object> dotted = new HashMap<>();
        dotted.put("db.system", "postgresql");
        dotted.put("server.address", "pg-host");
        dotted.put("server.port", 5432);

        assertThat(derivedName("SPAN_KIND_CLIENT", ipAddress), equalTo("postgresql"));
        assertTrue(isPublishableDependencyName(derivedName("SPAN_KIND_CLIENT", ipAddress)));
        assertThat(derivedName("SPAN_KIND_CLIENT", flattened), equalTo("postgresql:pg-host"));
        assertThat(derivedName("SPAN_KIND_CLIENT", dotted), equalTo(derivedName("SPAN_KIND_CLIENT", flattened)));
    }

    @Test
    void derivedRemoteService_forAwsSdkDatabaseCall_keepsAwsServiceName() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("rpc.system", "aws-api");
        attributes.put("rpc.service", "DynamoDb");
        attributes.put("rpc.method", "GetItem");
        attributes.put("db.system", "dynamodb");
        attributes.put("server.address", "dynamodb.us-east-1.amazonaws.com");

        assertThat(derivedName("SPAN_KIND_CLIENT", attributes), equalTo("AWS::DynamoDB"));
    }

    @Test
    void derivedRemoteService_forExternal_dropsSchemeDefaultPorts() {
        final Map<String, Object> fromUrl = new HashMap<>();
        fromUrl.put("http.request.method", "GET");
        fromUrl.put("url.full", "https://api.example.com/v1/charges");
        final Map<String, Object> explicitPort = new HashMap<>();
        explicitPort.put("http.request.method", "GET");
        explicitPort.put("server.address", "api.example.com");
        explicitPort.put("server.port", 443);
        final Map<String, Object> noPort = new HashMap<>();
        noPort.put("http.request.method", "GET");
        noPort.put("server.address", "api.example.com");
        final Map<String, Object> httpPort = new HashMap<>();
        httpPort.put("http.method", "GET");
        httpPort.put("http.url", "http://api.example.com/v1");
        final Map<String, Object> customPort = new HashMap<>();
        customPort.put("http.request.method", "GET");
        customPort.put("server.address", "api.example.com");
        customPort.put("server.port", 8443);

        assertThat(derivedName("SPAN_KIND_CLIENT", fromUrl), equalTo("api.example.com"));
        assertThat(derivedName("SPAN_KIND_CLIENT", explicitPort), equalTo("api.example.com"));
        assertThat(derivedName("SPAN_KIND_CLIENT", noPort), equalTo("api.example.com"));
        assertThat(derivedName("SPAN_KIND_CLIENT", httpPort), equalTo("api.example.com"));
        assertThat(derivedName("SPAN_KIND_CLIENT", customPort), equalTo("api.example.com:8443"));
    }

    @Test
    void derivedRemoteService_forExternalWithIpDerivedHostName_isNotPublishable() {
        for (final String host : new String[]{"ip-10-0-0-5.ec2.internal", "10-0-0-5.ns.pod.cluster.local",
                "ec2-1-2-3-4.compute-1.amazonaws.com"}) {
            final Map<String, Object> attributes = new HashMap<>();
            attributes.put("http.request.method", "GET");
            attributes.put("server.address", host);
            attributes.put("server.port", 8080);

            assertTrue(!isPublishableDependencyName(derivedName("SPAN_KIND_CLIENT", attributes)), host);
        }
    }

    @Test
    void derivedRemoteService_withCustomDenylist_suppressesMatchingHost() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("http.request.method", "GET");
        attributes.put("server.address", "egress-proxy");
        attributes.put("server.port", 3128);
        final SpanStateData span = new SpanStateData("service", "01", null, "02", "SPAN_KIND_CLIENT", "span", "op",
                1000L, "OK", "2023-01-01", null, attributes,
                new DependencyNamingPolicy(java.util.List.of("^egress-proxy$")));

        assertTrue(!isPublishableDependencyName(span.getDerivedRemoteService()));
    }

    @Test
    void isPublishableDependencyName_rejectsLoopbackNames() {
        assertTrue(!isPublishableDependencyName("localhost"));
        assertTrue(!isPublishableDependencyName("localhost:3500"));
        assertTrue(!isPublishableDependencyName("LOCALHOST:3500"));
        assertTrue(!isPublishableDependencyName("localhost.localdomain:80"));
        assertTrue(!isPublishableDependencyName("app.localhost:8080"));
        assertTrue(isPublishableDependencyName("localhost-api.example.com"));
    }

    @Test
    void messagingDestination_fallsBackToLegacyKey() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("messaging.system", "rabbitmq");
        attributes.put("messaging.destination", "invoices");

        final SpanStateData span = spanWithAttributes("SPAN_KIND_PRODUCER", attributes);

        assertThat(span.getMessagingDestination(), equalTo("invoices"));
        assertThat(span.getDerivedRemoteService(), equalTo("rabbitmq:invoices"));
        assertThat(span.getDependencyAttributes().get("messaging.destination.name"), equalTo("invoices"));
    }

    @Test
    void messagingOperation_fallsBackToOperationType() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("messaging.system", "kafka");
        attributes.put("messaging.destination.name", "orders");
        attributes.put("messaging.operation.type", "process");

        assertThat(spanWithAttributes("SPAN_KIND_CONSUMER", attributes).getMessagingOperation(), equalTo("process"));
    }

    @Test
    void nullNamingPolicy_skipsDependencyDerivation() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("db.system.name", "postgresql");
        attributes.put("messaging.system", "kafka");
        attributes.put("messaging.destination.name", "orders");

        final SpanStateData span = new SpanStateData("service", "01", null, "02", "SPAN_KIND_PRODUCER", "span", "op",
                1000L, "OK", "2023-01-01", null, attributes, null);

        assertNull(span.getDerivedNodeType());
        assertNull(span.getDerivedRemoteService());
        assertNull(span.getMessagingSystem());
        assertTrue(span.getDependencyAttributes().isEmpty());
    }

    @Test
    void dependencyAttributes_omitHostsThatCannotNameANode() {
        final Map<String, Object> ipHost = new HashMap<>();
        ipHost.put("db.system.name", "postgresql");
        ipHost.put("db.namespace", "orders");
        ipHost.put("server.address", "10.0.0.5");
        ipHost.put("server.port", 5432);
        final Map<String, Object> namedHost = new HashMap<>(ipHost);
        namedHost.put("server.address", "orders-db.internal");

        final Map<String, String> ipDeps = spanWithAttributes("SPAN_KIND_CLIENT", ipHost).getDependencyAttributes();
        final Map<String, String> namedDeps = spanWithAttributes("SPAN_KIND_CLIENT", namedHost).getDependencyAttributes();

        assertNull(ipDeps.get("server.address"));
        assertNull(ipDeps.get("server.port"));
        assertThat(ipDeps.get("db.namespace"), equalTo("orders"));
        assertThat(namedDeps.get("server.address"), equalTo("orders-db.internal"));
    }

    @Test
    void derivedRemoteService_forExternalUrlWithoutDefaultPort_dropsTheUnknownPortMarker() {
        final Map<String, Object> attributes = new HashMap<>();
        // java.net.URL knows "file" but gives it no default port, so the shared extractor names it "inventory:-1".
        attributes.put("http.request.method", "GET");
        attributes.put("url.full", "file://inventory/share/stock.csv");

        assertThat(derivedName("SPAN_KIND_CLIENT", attributes), equalTo("inventory"));
    }

    @Test
    void derivedRemoteService_forClientMessagingSpanWithoutDestination_isNotPublishable() {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("messaging.system", "kafka");
        attributes.put("messaging.operation", "settle");

        assertTrue(!isPublishableDependencyName(derivedName("SPAN_KIND_CLIENT", attributes)));
    }

    @Test
    void hostWithoutPort_stripsNumericAndUnknownPorts() {
        assertThat(SpanStateData.hostWithoutPort("api.example.com:443"), equalTo("api.example.com"));
        assertThat(SpanStateData.hostWithoutPort("inventory:-1"), equalTo("inventory"));
        assertThat(SpanStateData.hostWithoutPort("kafka:orders"), equalTo("kafka:orders"));
        assertThat(SpanStateData.hostWithoutPort("AWS::DynamoDB"), equalTo("AWS::DynamoDB"));
    }
}
