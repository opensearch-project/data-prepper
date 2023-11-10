package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutIndexTemplateResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ComposableTemplateAPIWrapperTest {
    @Mock
    private OpenSearchClient openSearchClient;
    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;
    @Mock
    private OpenSearchTransport openSearchTransport;
    @Mock
    private TransportOptions openSearchTransportOptions;
    @Mock
    private GetIndexTemplateResponse getIndexTemplateResponse;
    @Mock
    private BooleanResponse booleanResponse;
    @Captor
    private ArgumentCaptor<Map<String, Object>> putIndexTemplateRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<Endpoint<Map<String, Object>, PutIndexTemplateResponse, ErrorResponse>> endpointArgumentCaptor;
    @Captor
    private ArgumentCaptor<ExistsIndexTemplateRequest> existsIndexTemplateRequestArgumentCaptor;

    private String indexTemplateName;
    private ComposableTemplateAPIWrapper objectUnderTest;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
        objectUnderTest = new ComposableTemplateAPIWrapper(openSearchClient);
        lenient().when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        indexTemplateName = UUID.randomUUID().toString();
    }

    @Test
    void putTemplate_throws_if_template_is_not_ComposableIndexTemplate() {
        final IndexTemplate indexTemplate = mock(IndexTemplate.class);
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.putTemplate(indexTemplate));
    }

    @Test
    void putTemplate_performs_putIndexTemplate_request() throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);

        final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
        final IndexTemplate indexTemplate = new ComposableIndexTemplate(new HashMap<>());
        indexTemplate.setTemplateName(indexTemplateName);
        indexTemplate.setIndexPatterns(indexPatterns);
        objectUnderTest.putTemplate(indexTemplate);

        verify(openSearchTransport).performRequest(
                putIndexTemplateRequestArgumentCaptor.capture(),
                endpointArgumentCaptor.capture(),
                eq(openSearchTransportOptions)
        );

        Map<String, Object> actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

        assertThat(actualPutRequest.get("index_patterns"), equalTo(indexPatterns));

        Endpoint<Map<String, Object>, PutIndexTemplateResponse, ErrorResponse> actualEndpoint = endpointArgumentCaptor.getValue();
        assertThat(actualEndpoint.method(null), equalTo("PUT"));
        assertThat(actualEndpoint.requestUrl(null), equalTo("/_index_template/" + indexTemplateName));
        assertThat(actualEndpoint.queryParameters(null), equalTo(Collections.emptyMap()));
        assertThat(actualEndpoint.headers(null), equalTo(Collections.emptyMap()));
        assertThat(actualEndpoint.hasRequestBody(), equalTo(true));

        assertThat(actualEndpoint, instanceOf(JsonEndpoint.class));
        assertThat(((JsonEndpoint)actualEndpoint).responseDeserializer(), equalTo(PutIndexTemplateResponse._DESERIALIZER));
    }

    @Test
    void getExistingTemplate_should_calls_existIndexTemplate_with_templateName() throws IOException {
        when(booleanResponse.value()).thenReturn(false);
        when(openSearchIndicesClient.existsIndexTemplate(any(ExistsIndexTemplateRequest.class)))
                .thenReturn(booleanResponse);

        objectUnderTest.getTemplate(indexTemplateName);

        verify(openSearchIndicesClient).existsIndexTemplate(existsIndexTemplateRequestArgumentCaptor.capture());

        final ExistsIndexTemplateRequest actualRequest = existsIndexTemplateRequestArgumentCaptor.getValue();
        assertThat(actualRequest.name(), notNullValue());
        assertThat(actualRequest.name(), equalTo(indexTemplateName));
    }

    @Test
    void getExistingTemplate_should_return_empty_if_no_index_template_exists() throws IOException {
        when(booleanResponse.value()).thenReturn(false);
        when(openSearchIndicesClient.existsIndexTemplate(any(ExistsIndexTemplateRequest.class)))
                .thenReturn(booleanResponse);

        final Optional<GetIndexTemplateResponse> optionalGetIndexTemplateResponse = objectUnderTest.getTemplate(
                indexTemplateName);

        assertThat(optionalGetIndexTemplateResponse, notNullValue());
        assertThat(optionalGetIndexTemplateResponse.isPresent(), equalTo(false));
    }

    @Test
    void getExistingTemplate_should_return_template_if_template_exists() throws IOException {
        when(booleanResponse.value()).thenReturn(true);
        when(openSearchIndicesClient.existsIndexTemplate(any(ExistsIndexTemplateRequest.class)))
                .thenReturn(booleanResponse);
        when(openSearchIndicesClient.getIndexTemplate(any(GetIndexTemplateRequest.class)))
                .thenReturn(getIndexTemplateResponse);

        final Optional<GetIndexTemplateResponse> optionalGetIndexTemplateResponse = objectUnderTest.getTemplate(
                indexTemplateName);

        assertThat(optionalGetIndexTemplateResponse, notNullValue());
        assertThat(optionalGetIndexTemplateResponse.isPresent(), equalTo(true));
        assertThat(optionalGetIndexTemplateResponse.get(), equalTo(getIndexTemplateResponse));
    }


    @Nested
    class IndexTemplateWithCreateTemplateTests {
        private List<String> indexPatterns;

        @BeforeEach
        void setUp() {
            when(openSearchClient._transport()).thenReturn(openSearchTransport);

            indexPatterns = Collections.singletonList(UUID.randomUUID().toString());

            when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);
        }

        @Test
        void putTemplate_with_setTemplateName_performs_putIndexTemplate_request() throws IOException {
            final IndexTemplate indexTemplate = new ComposableIndexTemplate(new HashMap<>());
            indexTemplate.setTemplateName(indexTemplateName);
            objectUnderTest.putTemplate(indexTemplate);

            verify(openSearchTransport).performRequest(
                    putIndexTemplateRequestArgumentCaptor.capture(),
                    endpointArgumentCaptor.capture(),
                    eq(openSearchTransportOptions)
            );

            final Map<String, Object> actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

            final Endpoint<Map<String, Object>, PutIndexTemplateResponse, ErrorResponse> actualEndpoint = endpointArgumentCaptor.getValue();
            assertThat(actualEndpoint.method(null), equalTo("PUT"));
            assertThat(actualEndpoint.requestUrl(null), equalTo("/_index_template/" + indexTemplateName));
            assertThat(actualEndpoint.queryParameters(null), equalTo(Collections.emptyMap()));
            assertThat(actualEndpoint.headers(null), equalTo(Collections.emptyMap()));
            assertThat(actualEndpoint.hasRequestBody(), equalTo(true));

            assertThat(actualEndpoint, instanceOf(JsonEndpoint.class));
            assertThat(((JsonEndpoint)actualEndpoint).responseDeserializer(), equalTo(PutIndexTemplateResponse._DESERIALIZER));

            assertThat(actualPutRequest.get("version"), nullValue());
            assertThat(actualPutRequest.get("index_patterns"), nullValue());
            assertThat(actualPutRequest.get("template"), nullValue());
            assertThat(actualPutRequest.get("priority"), nullValue());
            assertThat(actualPutRequest.get("composed_of"), nullValue());
        }

        @Test
        void putTemplate_with_setIndexPatterns_performs_putIndexTemplate_request() throws IOException {
            final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());

            final IndexTemplate indexTemplate = new ComposableIndexTemplate(new HashMap<>());
            indexTemplate.setTemplateName(indexTemplateName);
            indexTemplate.setIndexPatterns(indexPatterns);
            objectUnderTest.putTemplate(indexTemplate);

            verify(openSearchTransport).performRequest(
                    putIndexTemplateRequestArgumentCaptor.capture(),
                    endpointArgumentCaptor.capture(),
                    eq(openSearchTransportOptions)
            );

            Map<String, Object> actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

            assertThat(actualPutRequest.get("index_patterns"), equalTo(indexPatterns));

            Endpoint<Map<String, Object>, PutIndexTemplateResponse, ErrorResponse> actualEndpoint = endpointArgumentCaptor.getValue();
            assertThat(actualEndpoint.method(null), equalTo("PUT"));
            assertThat(actualEndpoint.requestUrl(null), equalTo("/_index_template/" + indexTemplateName));
            assertThat(actualEndpoint.queryParameters(null), equalTo(Collections.emptyMap()));
            assertThat(actualEndpoint.headers(null), equalTo(Collections.emptyMap()));
            assertThat(actualEndpoint.hasRequestBody(), equalTo(true));

            assertThat(actualEndpoint, instanceOf(JsonEndpoint.class));
            assertThat(((JsonEndpoint)actualEndpoint).responseDeserializer(), equalTo(PutIndexTemplateResponse._DESERIALIZER));

            assertThat(actualPutRequest, not(hasKey("template")));
            assertThat(actualPutRequest, not(hasKey("priority")));
            assertThat(actualPutRequest, not(hasKey("composedOf")));
            assertThat(actualPutRequest, not(hasKey("template")));
        }

        @Test
        void putTemplate_with_defined_template_values_performs_putIndexTemplate_request() throws IOException {
            final Long version = (long) (random.nextInt(10_000) + 100);
            final int priority = random.nextInt(1000) + 100;
            final String numberOfShards = Integer.toString(random.nextInt(1000) + 100);
            final List<String> composedOf = Collections.singletonList(UUID.randomUUID().toString());

            final IndexTemplate indexTemplate = new ComposableIndexTemplate(
                    Map.of("version", version,
                            "priority", priority,
                            "composed_of", composedOf,
                            "template", Map.of(
                                    "settings", Map.of(
                                            "index", Map.of("number_of_shards", numberOfShards)),
                                    "mappings", Map.of("date_detection", true)
                            )
                    ));
            indexTemplate.setTemplateName(indexTemplateName);
            indexTemplate.setIndexPatterns(indexPatterns);
            objectUnderTest.putTemplate(indexTemplate);

            verify(openSearchTransport).performRequest(
                    putIndexTemplateRequestArgumentCaptor.capture(),
                    endpointArgumentCaptor.capture(),
                    eq(openSearchTransportOptions)
            );

            final Map<String, Object> actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

            assertThat(actualPutRequest.get("index_patterns"), equalTo(indexPatterns));

            final Endpoint<Map<String, Object>, PutIndexTemplateResponse, ErrorResponse> actualEndpoint = endpointArgumentCaptor.getValue();
            assertThat(actualEndpoint.method(null), equalTo("PUT"));
            assertThat(actualEndpoint.requestUrl(null), equalTo("/_index_template/" + indexTemplateName));
            assertThat(actualEndpoint.queryParameters(null), equalTo(Collections.emptyMap()));
            assertThat(actualEndpoint.headers(null), equalTo(Collections.emptyMap()));
            assertThat(actualEndpoint.hasRequestBody(), equalTo(true));

            assertThat(actualEndpoint, instanceOf(JsonEndpoint.class));
            assertThat(((JsonEndpoint)actualEndpoint).responseDeserializer(), equalTo(PutIndexTemplateResponse._DESERIALIZER));

            assertThat(actualPutRequest, hasKey("template"));
            assertThat(actualPutRequest.get("version"), equalTo(version));
            assertThat(actualPutRequest.get("priority"), equalTo(priority));
            assertThat(actualPutRequest.get("composed_of"), equalTo(composedOf));
            assertThat(actualPutRequest.get("template"), notNullValue());
            assertThat(actualPutRequest.get("template"), instanceOf(Map.class));
            Map<String, Object> actualTemplate = (Map<String, Object>) actualPutRequest.get("template");
            assertThat(actualTemplate.get("mappings"), instanceOf(Map.class));
            assertThat(((Map) actualTemplate.get("mappings")).get("date_detection"), equalTo(true));
            assertThat(actualTemplate.get("settings"), instanceOf(Map.class));
            Map<String, Object> actualSettings = (Map<String, Object>) actualTemplate.get("settings");
            assertThat(actualSettings.get("index"), instanceOf(Map.class));
            Map<String, Object> actualIndex = (Map<String, Object>) actualSettings.get("index");
            assertThat(actualIndex.get("number_of_shards"), equalTo(numberOfShards));
        }
    }
}