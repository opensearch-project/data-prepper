/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.cluster.ExistsComponentTemplateRequest;
import org.opensearch.client.opensearch.cluster.OpenSearchClusterClient;
import org.opensearch.client.opensearch.cluster.PutComponentTemplateResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ComponentTemplateAPIWrapperTest {
    @Mock
    private OpenSearchClient openSearchClient;
    @Mock
    private OpenSearchClusterClient openSearchClusterClient;
    @Mock
    private OpenSearchTransport openSearchTransport;
    @Mock
    private TransportOptions openSearchTransportOptions;
    @Captor
    private ArgumentCaptor<ExistsComponentTemplateRequest> existsRequestCaptor;
    @Captor
    private ArgumentCaptor<Endpoint<Void, JsonData, ErrorResponse>> getEndpointCaptor;
    @Captor
    private ArgumentCaptor<Map<String, Object>> putBodyCaptor;
    @Captor
    private ArgumentCaptor<Endpoint<Map<String, Object>, PutComponentTemplateResponse, ErrorResponse>> endpointCaptor;

    private String componentTemplateName;

    @BeforeEach
    void setUp() {
        componentTemplateName = UUID.randomUUID().toString();
    }

    private ComponentTemplateAPIWrapper createObjectUnderTest() {
        return new ComponentTemplateAPIWrapper(openSearchClient);
    }

    @Test
    void getComponentTemplateVersion_returns_empty_when_component_template_does_not_exist() throws IOException {
        when(openSearchClient.cluster()).thenReturn(openSearchClusterClient);
        when(openSearchClusterClient.existsComponentTemplate(any(ExistsComponentTemplateRequest.class)))
                .thenReturn(new BooleanResponse(false));

        assertThat(createObjectUnderTest().getComponentTemplateVersion(componentTemplateName), equalTo(Optional.empty()));

        verify(openSearchClusterClient).existsComponentTemplate(existsRequestCaptor.capture());
        assertThat(existsRequestCaptor.getValue().name(), equalTo(List.of(componentTemplateName)));
        verifyNoInteractions(openSearchTransport);
    }

    @Test
    void getComponentTemplateVersion_returns_version_of_existing_component_template() throws IOException {
        stubGetResponse("{\"component_templates\":[{\"name\":\"" + componentTemplateName + "\",\"component_template\":"
                + "{\"template\":{\"mappings\":{}},\"version\":3}}]}");

        assertThat(createObjectUnderTest().getComponentTemplateVersion(componentTemplateName), equalTo(Optional.of(3L)));

        verify(openSearchTransport).performRequest(isNull(), getEndpointCaptor.capture(), eq(openSearchTransportOptions));
        final Endpoint<Void, JsonData, ErrorResponse> endpoint = getEndpointCaptor.getValue();
        assertThat(endpoint.method(null), equalTo("GET"));
        assertThat(endpoint.requestUrl(null), equalTo("/_component_template/" + componentTemplateName));
        assertThat(endpoint.queryParameters(null), equalTo(Collections.emptyMap()));
        assertThat(endpoint.hasRequestBody(), equalTo(false));
    }

    @Test
    void getComponentTemplateVersion_returns_zero_when_existing_component_template_has_no_version() throws IOException {
        stubGetResponse("{\"component_templates\":[{\"name\":\"" + componentTemplateName + "\",\"component_template\":"
                + "{\"template\":{\"mappings\":{}}}}]}");

        assertThat(createObjectUnderTest().getComponentTemplateVersion(componentTemplateName), equalTo(Optional.of(0L)));
    }

    @Test
    void getComponentTemplateVersion_throws_when_response_has_multiple_component_templates() throws IOException {
        stubGetResponse("{\"component_templates\":[{\"name\":\"a\",\"component_template\":{\"template\":{}}},"
                + "{\"name\":\"b\",\"component_template\":{\"template\":{}}}]}");

        final ComponentTemplateAPIWrapper objectUnderTest = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.getComponentTemplateVersion(componentTemplateName));
    }

    @Test
    void putComponentTemplate_performs_put_component_template_request() throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);
        final Map<String, Object> body = Map.of("version", 1, "template", Map.of("mappings", Map.of()));

        createObjectUnderTest().putComponentTemplate(componentTemplateName, body);

        verify(openSearchTransport).performRequest(putBodyCaptor.capture(), endpointCaptor.capture(), eq(openSearchTransportOptions));
        assertThat(putBodyCaptor.getValue(), equalTo(body));

        final Endpoint<Map<String, Object>, PutComponentTemplateResponse, ErrorResponse> endpoint = endpointCaptor.getValue();
        assertThat(endpoint.method(null), equalTo("PUT"));
        assertThat(endpoint.requestUrl(null), equalTo("/_component_template/" + componentTemplateName));
        assertThat(endpoint.queryParameters(null), equalTo(Collections.emptyMap()));
        assertThat(endpoint.headers(null), equalTo(Collections.emptyMap()));
        assertThat(endpoint.hasRequestBody(), equalTo(true));
        assertThat(endpoint, instanceOf(JsonEndpoint.class));
        assertThat(((JsonEndpoint) endpoint).responseDeserializer(), equalTo(PutComponentTemplateResponse._DESERIALIZER));
    }

    private void stubGetResponse(final String responseBody) throws IOException {
        when(openSearchClient.cluster()).thenReturn(openSearchClusterClient);
        when(openSearchClusterClient.existsComponentTemplate(any(ExistsComponentTemplateRequest.class)))
                .thenReturn(new BooleanResponse(true));
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);
        when(openSearchTransport.performRequest(isNull(), any(), eq(openSearchTransportOptions)))
                .thenReturn(toJsonData(responseBody));
    }

    static JsonData toJsonData(final String json) {
        final JacksonJsonpMapper mapper = new JacksonJsonpMapper();
        return JsonData._DESERIALIZER.deserialize(mapper.jsonProvider().createParser(new StringReader(json)), mapper);
    }
}
