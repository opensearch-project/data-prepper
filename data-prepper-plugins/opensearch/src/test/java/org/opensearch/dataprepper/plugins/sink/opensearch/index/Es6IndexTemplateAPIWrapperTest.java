package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;
import org.opensearch.client.opensearch.indices.PutTemplateResponse;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Es6IndexTemplateAPIWrapperTest {
    @Mock
    private OpenSearchClient openSearchClient;
    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;
    @Mock
    private OpenSearchTransport openSearchTransport;
    @Mock
    private GetTemplateResponse getTemplateResponse;
    @Mock
    private BooleanResponse booleanResponse;
    @Mock
    private TransportOptions transportOptions;
    @Captor
    private ArgumentCaptor<PutTemplateRequest> putTemplateRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse>> jsonEndpointArgumentCaptor;
    @Captor
    private ArgumentCaptor<ExistsTemplateRequest> existsTemplateRequestArgumentCaptor;

    private String templateName;
    private Es6IndexTemplateAPIWrapper objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new Es6IndexTemplateAPIWrapper(openSearchClient);
        lenient().when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        templateName = UUID.randomUUID().toString();
    }

    @Test
    void putTemplate_throws_if_template_is_not_LegacyIndexTemplate() {
        final IndexTemplate indexTemplate = mock(IndexTemplate.class);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.putTemplate(
                indexTemplate));
    }

    @Test
    void putTemplate_performs_perform_request() throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchIndicesClient._transport()).thenReturn(openSearchTransport);
        when(openSearchIndicesClient._transportOptions()).thenReturn(transportOptions);
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());

        final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
        final IndexTemplate indexTemplate = new LegacyIndexTemplate(new HashMap<>());
        indexTemplate.setTemplateName(templateName);
        indexTemplate.setIndexPatterns(indexPatterns);
        objectUnderTest.putTemplate(indexTemplate);

        verify(openSearchTransport).performRequest(
                putTemplateRequestArgumentCaptor.capture(), jsonEndpointArgumentCaptor.capture(), eq(transportOptions));

        final PutTemplateRequest actualPutRequest = putTemplateRequestArgumentCaptor.getValue();

        assertThat(actualPutRequest.name(), equalTo(templateName));
        assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
        final JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse> endpoint = jsonEndpointArgumentCaptor.getValue();
        assertThat(endpoint.requestUrl(actualPutRequest), equalTo(
                String.format("/_template/%s?include_type_name=false", templateName)));
    }

    @Test
    void getExistingTemplate_should_calls_existTemplate_with_templateName() throws IOException {
        when(booleanResponse.value()).thenReturn(false);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class)))
                .thenReturn(booleanResponse);

        objectUnderTest.getTemplate(templateName);

        verify(openSearchIndicesClient).existsTemplate(existsTemplateRequestArgumentCaptor.capture());

        final ExistsTemplateRequest actualRequest = existsTemplateRequestArgumentCaptor.getValue();
        assertThat(actualRequest.name(), notNullValue());
        assertAll(
                () -> assertThat(actualRequest.name().size(), equalTo(1)),
                () -> assertThat(actualRequest.name(), hasItem(templateName))
        );
    }

    @Test
    void getExistingTemplate_should_return_empty_if_no_template_exists() throws IOException {
        when(booleanResponse.value()).thenReturn(false);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class)))
                .thenReturn(booleanResponse);

        final Optional<GetTemplateResponse> optionalGetTemplateResponse = objectUnderTest.getTemplate(templateName);

        assertThat(optionalGetTemplateResponse, notNullValue());
        assertThat(optionalGetTemplateResponse.isPresent(), equalTo(false));
    }

    @Test
    void getExistingTemplate_should_return_template_if_template_exists() throws IOException {
        when(booleanResponse.value()).thenReturn(true);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class)))
                .thenReturn(booleanResponse);
        when(openSearchIndicesClient.getTemplate(any(GetTemplateRequest.class)))
                .thenReturn(getTemplateResponse);

        final Optional<GetTemplateResponse> optionalGetTemplateResponse = objectUnderTest.getTemplate(templateName);

        assertThat(optionalGetTemplateResponse, notNullValue());
        assertThat(optionalGetTemplateResponse.isPresent(), equalTo(true));
        assertThat(optionalGetTemplateResponse.get(), equalTo(getTemplateResponse));
    }
}