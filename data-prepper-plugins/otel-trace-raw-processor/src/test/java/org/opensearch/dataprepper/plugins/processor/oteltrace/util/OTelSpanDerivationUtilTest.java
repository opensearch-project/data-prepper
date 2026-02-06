/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.opensearch.dataprepper.model.trace.Span;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OTelSpanDerivationUtilTest {

    private List<Span> spans;
    private Span serverSpan;
    private Span clientSpan;
    private Map<String, Object> spanAttributes;

    @BeforeEach
    void setUp() {
        spans = new ArrayList<>();
        serverSpan = mock(Span.class);
        clientSpan = mock(Span.class);
        spanAttributes = new HashMap<>();
    }

    @Test
    void testDeriveServerSpanAttributes_withNullSpans_shouldReturnSafely() {
        // Should not throw exception
        OTelSpanDerivationUtil.deriveServerSpanAttributes(null);
    }

    @Test
    void testDeriveServerSpanAttributes_withEmptyList_shouldReturnSafely() {
        // Should not throw exception
        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);
    }

    @Test
    void testDeriveServerSpanAttributes_withNonServerSpan_shouldSkipDerivation() {
        when(clientSpan.getKind()).thenReturn("CLIENT");
        when(clientSpan.getAttributes()).thenReturn(spanAttributes);
        spans.add(clientSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        // CLIENT span should not have derived attributes added
        assertNull(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE));
        assertNull(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE));
        assertNull(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE));
        assertNull(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE));
    }

    @Test
    void testDeriveServerSpanAttributes_withServerSpan_shouldAddDerivedAttributes() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("GET /users");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        // SERVER span should have derived attributes
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), notNullValue());
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), notNullValue());
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), notNullValue());
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE), notNullValue());
    }

    @Test
    void testErrorAndFaultDerivation_withNoErrors_shouldSetBothToZero() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("0"));
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("0"));
    }

    @Test
    void testErrorAndFaultDerivation_withSpanStatusError_shouldSetFaultToOne() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "ERROR");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("1"));
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("0"));
    }

    @Test
    void testErrorAndFaultDerivation_withHttp4xxStatus_shouldSetErrorToOne() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");
        spanAttributes.put("http.response.status_code", 404);
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("0"));
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("1"));
    }

    @Test
    void testErrorAndFaultDerivation_withHttp5xxStatus_shouldSetFaultToOne() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");
        spanAttributes.put("http.response.status_code", 500);
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("1"));
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("0"));
    }

    @Test
    void testErrorAndFaultDerivation_withLegacyHttpStatusCode_shouldWork() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");
        spanAttributes.put("http.status_code", "404");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("0"));
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("1"));
    }

    @Test
    void testOperationNameDerivation_withSpanName_shouldUseSpanName() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("custom-operation");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("custom-operation"));
    }

    @Test
    void testOperationNameDerivation_withHttpMethodAndPath_shouldUseHttpDerivation() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("GET"); // Name equals HTTP method
        spanAttributes.put("http.request.method", "GET");
        spanAttributes.put("http.path", "/users/123");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("GET /users"));
    }

    @Test
    void testOperationNameDerivation_withUnknownOperation_shouldUseHttpDerivation() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("UnknownOperation");
        spanAttributes.put("http.request.method", "POST");
        spanAttributes.put("http.target", "/api/orders/456");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("POST /api"));
    }

    @Test
    void testOperationNameDerivation_withMultiplePathLevels_shouldExtractFirstSection() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("UnknownOperation");
        spanAttributes.put("http.method", "PUT");
        spanAttributes.put("http.url", "/api/v1/users/123/profile?includeDetails=true");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("PUT /api"));
    }

    @Test
    void testOperationNameDerivation_withMissingHttpInfo_shouldReturnUnknownOperation() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("UnknownOperation");
        // No HTTP method or URL attributes
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("UnknownOperation"));
    }

    @Test
    void testEnvironmentDerivation_withDeploymentEnvironmentName_shouldUseIt() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");

        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment.name", "production");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        spanAttributes.put("resource", resource);
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE), equalTo("production"));
    }

    @Test
    void testEnvironmentDerivation_withDeploymentEnvironment_shouldUseIt() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");

        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment", "staging");

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        spanAttributes.put("resource", resource);
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE), equalTo("staging"));
    }

    @Test
    void testEnvironmentDerivation_withNoResource_shouldUseDefault() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE), equalTo("generic:default"));
    }

    @Test
    void testEnvironmentDerivation_preferenceOrder_shouldPreferEnvironmentName() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");

        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment.name", "production");
        resourceAttributes.put("deployment.environment", "staging"); // Should not be used

        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);

        spanAttributes.put("resource", resource);
        spans.add(serverSpan);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE), equalTo("production"));
    }

    @Test
    void testMixedSpanTypes_shouldOnlyDeriveForServerSpans() {
        Span serverSpan1 = mock(Span.class);
        Span clientSpan1 = mock(Span.class);
        Span serverSpan2 = mock(Span.class);

        Map<String, Object> serverAttributes1 = new HashMap<>();
        Map<String, Object> clientAttributes1 = new HashMap<>();
        Map<String, Object> serverAttributes2 = new HashMap<>();

        Map<String, Object> status1 = new HashMap<>();
        status1.put("code", "OK");
        Map<String, Object> status2 = new HashMap<>();
        status2.put("code", "ERROR");

        when(serverSpan1.getKind()).thenReturn("SERVER");
        when(serverSpan1.getAttributes()).thenReturn(serverAttributes1);
        when(serverSpan1.getStatus()).thenReturn(status1);
        when(serverSpan1.getName()).thenReturn("server-span-1");

        when(clientSpan1.getKind()).thenReturn("CLIENT");
        when(clientSpan1.getAttributes()).thenReturn(clientAttributes1);

        when(serverSpan2.getKind()).thenReturn("SERVER");
        when(serverSpan2.getAttributes()).thenReturn(serverAttributes2);
        when(serverSpan2.getStatus()).thenReturn(status2);
        when(serverSpan2.getName()).thenReturn("server-span-2");

        spans.add(serverSpan1);
        spans.add(clientSpan1);
        spans.add(serverSpan2);

        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);

        // Server spans should have derived attributes
        assertThat(serverAttributes1.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("server-span-1"));
        assertThat(serverAttributes2.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("1"));

        // Client span should not have derived attributes
        assertNull(clientAttributes1.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE));
        assertNull(clientAttributes1.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE));
        assertNull(clientAttributes1.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE));
        assertNull(clientAttributes1.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE));
    }

    @Test
    void testHttpStatusCodeParsing_withVariousTypes_shouldParseCorrectly() {
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        when(serverSpan.getKind()).thenReturn("SERVER");
        when(serverSpan.getAttributes()).thenReturn(spanAttributes);
        when(serverSpan.getStatus()).thenReturn(status);
        when(serverSpan.getName()).thenReturn("test-span");
        spans.add(serverSpan);

        // Test with Long
        spanAttributes.put("http.response.status_code", 404L);
        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("1"));

        // Reset and test with String
        spanAttributes.clear();
        spanAttributes.put("http.response.status_code", "500");
        OTelSpanDerivationUtil.deriveServerSpanAttributes(spans);
        assertThat(spanAttributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("1"));
    }
}
