/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.util;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.util.OTelSpanDerivationUtil.ErrorFaultResult;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OTelSpanDerivationUtilTest {

    @Test
    void testComputeErrorAndFault_Http500() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 500);

        ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(1, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_Http404() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 404);

        ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(0, result.fault);
        assertEquals(1, result.error);
    }

    @Test
    void testComputeErrorAndFault_Http200() {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", 200);

        ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault((String) null, spanAttributes);

        assertEquals(0, result.fault);
        assertEquals(0, result.error);
    }

    @Test
    void testComputeErrorAndFault_SpanStatusError() {
        ErrorFaultResult result = OTelSpanDerivationUtil.computeErrorAndFault("ERROR", null);

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
    void testComputeEnvironment_Default() {
        String result = OTelSpanDerivationUtil.computeEnvironment(null);

        assertEquals("generic:default", result);
    }

    @Test
    void testExtractFirstPathSection() {
        assertEquals("/payment", OTelSpanDerivationUtil.extractFirstPathSection("/payment/123/details"));
        assertEquals("/api", OTelSpanDerivationUtil.extractFirstPathSection("/api"));
        assertEquals("/", OTelSpanDerivationUtil.extractFirstPathSection("/"));
        assertEquals("/", OTelSpanDerivationUtil.extractFirstPathSection(null));
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
    void testIsSpanStatusError() {
        assertTrue(OTelSpanDerivationUtil.isSpanStatusError("ERROR"));
        assertTrue(OTelSpanDerivationUtil.isSpanStatusError("2"));
        assertTrue(OTelSpanDerivationUtil.isSpanStatusError("error"));
        assertFalse(OTelSpanDerivationUtil.isSpanStatusError("OK"));
        assertFalse(OTelSpanDerivationUtil.isSpanStatusError("1"));
        assertFalse(OTelSpanDerivationUtil.isSpanStatusError((String) null));
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
    void testIsNonEmptyString() {
        assertTrue(OTelSpanDerivationUtil.isNonEmptyString("test"));
        assertTrue(OTelSpanDerivationUtil.isNonEmptyString(" test "));
        assertFalse(OTelSpanDerivationUtil.isNonEmptyString(""));
        assertFalse(OTelSpanDerivationUtil.isNonEmptyString("   "));
        assertFalse(OTelSpanDerivationUtil.isNonEmptyString(null));
    }
}