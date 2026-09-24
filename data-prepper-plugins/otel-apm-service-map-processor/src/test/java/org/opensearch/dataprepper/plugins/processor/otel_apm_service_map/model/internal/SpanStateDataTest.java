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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void derivedNodeType_isNullForPlainService() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("thread.id", 7);

        assertNull(spanWithAttributes("SPAN_KIND_SERVER", attributes).getDerivedNodeType());
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
}
