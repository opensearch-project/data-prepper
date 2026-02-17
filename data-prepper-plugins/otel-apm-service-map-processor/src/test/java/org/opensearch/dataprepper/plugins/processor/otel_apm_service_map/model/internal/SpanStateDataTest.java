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

class SpanStateDataTest {

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
