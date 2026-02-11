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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OTelSpanDerivationUtilTest {

    @Test
    void testComputeErrorAndFault_Http500() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 500);

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(1, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_Http404() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 404);

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(0, result.fault);
        assertEquals(1, result.error);
    }

    @Test
    void testComputeErrorAndFault_Http200() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 200);

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(0, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_SpanStatusError() {
        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault("ERROR", null);

        assertEquals(1, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_MapVersion() {
        Map<String, Object> spanStatus = new HashMap<>();
        spanStatus.put("code", "ERROR");

        OTelSpanDerivationUtil.ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault(spanStatus, null);

        assertEquals(1, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeOperationName_HttpDerivation() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.request.method", "GET");
        spanAttributes.put("http.path", "/payment/123");

        String result = OTelSpanDerivationUtil.computeOperationName("GET", spanAttributes);

        assertEquals("GET /payment", result);
    }

    @Test
    void testComputeOperationName_NoHttpDerivation() {
        Map<String, Object> spanAttributes = new HashMap<>();

        String result = OTelSpanDerivationUtil.computeOperationName("CustomOperation", spanAttributes);

        assertEquals("CustomOperation", result);
    }

    @Test
    void testComputeOperationName_FallbackToHttpMethod() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.method", "POST");
        spanAttributes.put("http.path", "/api/users");

        String result = OTelSpanDerivationUtil.computeOperationName("POST", spanAttributes);

        assertEquals("POST /api", result);
    }

    @Test
    void testComputeOperationName_UnknownOperation() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.request.method", "GET");

        String result = OTelSpanDerivationUtil.computeOperationName("GET", spanAttributes);

        assertEquals("UnknownOperation", result);
    }

    @Test
    void testComputeEnvironment_DeploymentEnvironmentName() {
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment.name", "production");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("production", result);
    }

    @Test
    void testComputeEnvironment_DeploymentEnvironment() {
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment", "staging");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", resource);

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("staging", result);
    }

    @Test
    void testComputeEnvironment_Default() {
        String result = OTelSpanDerivationUtil.computeEnvironment(null);

        assertEquals("generic:default", result);
    }

    @Test
    void testComputeEnvironment_NoResourceAttributes() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", new HashMap<>());

        String result = OTelSpanDerivationUtil.computeEnvironment(spanAttributes);

        assertEquals("generic:default", result);
    }


    @Test
    void testParseHttpStatusCode() {
        assertEquals(Integer.valueOf(200), OTelSpanDerivationUtil.parseHttpStatusCode(200));
        assertEquals(Integer.valueOf(404), OTelSpanDerivationUtil.parseHttpStatusCode("404"));
        assertEquals(Integer.valueOf(500), OTelSpanDerivationUtil.parseHttpStatusCode(500L));
        assertNull(OTelSpanDerivationUtil.parseHttpStatusCode("invalid"));
        assertNull(OTelSpanDerivationUtil.parseHttpStatusCode(null));
    }


    @Test
    void testGetStringAttribute() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("key1", "value1");
        attributes.put("key2", 123);
        attributes.put("key3", null);

        assertEquals("value1", OTelSpanDerivationUtil.getStringAttribute(attributes, "key1"));
        assertEquals("123", OTelSpanDerivationUtil.getStringAttribute(attributes, "key2"));
        assertNull(OTelSpanDerivationUtil.getStringAttribute(attributes, "key3"));
        assertNull(OTelSpanDerivationUtil.getStringAttribute(attributes, "nonexistent"));
        assertNull(OTelSpanDerivationUtil.getStringAttribute(null, "key1"));
    }

    @Test
    void testGetStringAttributeFromMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 123);

        assertEquals("value1", OTelSpanDerivationUtil.getStringAttributeFromMap(map, "key1"));
        assertEquals("123", OTelSpanDerivationUtil.getStringAttributeFromMap(map, "key2"));
        assertNull(OTelSpanDerivationUtil.getStringAttributeFromMap(null, "key1"));
    }


    @Test
    void testErrorFaultResult() {
        OTelSpanDerivationUtil.ErrorFaultResult result = new OTelSpanDerivationUtil.ErrorFaultResult(1, 0);
        assertEquals(1, result.error);
        assertEquals(0, result.fault);
    }
}
