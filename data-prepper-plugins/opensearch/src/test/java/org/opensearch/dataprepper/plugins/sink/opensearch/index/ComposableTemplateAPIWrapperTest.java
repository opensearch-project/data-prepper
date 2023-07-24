package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.get_index_template.IndexTemplateItem;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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
    private GetIndexTemplateResponse getIndexTemplateResponse;
    @Mock
    private BooleanResponse booleanResponse;
    @Captor
    private ArgumentCaptor<PutIndexTemplateRequest> putIndexTemplateRequestArgumentCaptor;
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
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());

        final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
        final IndexTemplate indexTemplate = new ComposableIndexTemplate(new HashMap<>());
        indexTemplate.setTemplateName(indexTemplateName);
        indexTemplate.setIndexPatterns(indexPatterns);
        objectUnderTest.putTemplate(indexTemplate);

        verify(openSearchIndicesClient).putIndexTemplate(putIndexTemplateRequestArgumentCaptor.capture());

        final PutIndexTemplateRequest actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

        assertThat(actualPutRequest.name(), equalTo(indexTemplateName));
        assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
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

        final Optional<IndexTemplateItem> optionalIndexTemplateItem = objectUnderTest.getTemplate(indexTemplateName);

        assertThat(optionalIndexTemplateItem, notNullValue());
        assertThat(optionalIndexTemplateItem.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void getExistingTemplate_should_throw_if_get_template_returns_unexpected_number_of_templates(final int numberOfTemplatesReturned) throws IOException {
        when(booleanResponse.value()).thenReturn(true);
        when(openSearchIndicesClient.existsIndexTemplate(any(ExistsIndexTemplateRequest.class)))
                .thenReturn(booleanResponse);
        final List<IndexTemplateItem> indexTemplateItems = mock(List.class);
        when(indexTemplateItems.size()).thenReturn(numberOfTemplatesReturned);
        when(getIndexTemplateResponse.indexTemplates()).thenReturn(indexTemplateItems);
        when(openSearchIndicesClient.getIndexTemplate(any(GetIndexTemplateRequest.class)))
                .thenReturn(getIndexTemplateResponse);

        assertThrows(RuntimeException.class, () -> objectUnderTest.getTemplate(indexTemplateName));

        verify(openSearchIndicesClient).getIndexTemplate(any(GetIndexTemplateRequest.class));
    }

    @Test
    void getExistingTemplate_should_return_template_if_template_exists() throws IOException {
        when(booleanResponse.value()).thenReturn(true);
        when(openSearchIndicesClient.existsIndexTemplate(any(ExistsIndexTemplateRequest.class)))
                .thenReturn(booleanResponse);
        final IndexTemplateItem indexTemplateItem = mock(IndexTemplateItem.class);
        when(getIndexTemplateResponse.indexTemplates()).thenReturn(Collections.singletonList(indexTemplateItem));
        when(openSearchIndicesClient.getIndexTemplate(any(GetIndexTemplateRequest.class)))
                .thenReturn(getIndexTemplateResponse);

        final Optional<IndexTemplateItem> optionalIndexTemplateItem = objectUnderTest.getTemplate(indexTemplateName);

        assertThat(optionalIndexTemplateItem, notNullValue());
        assertThat(optionalIndexTemplateItem.isPresent(), equalTo(true));
        assertThat(optionalIndexTemplateItem.get(), equalTo(indexTemplateItem));
    }


    @Nested
    class IndexTemplateWithCreateTemplateTests {
        private ArgumentCaptor<PutIndexTemplateRequest> putIndexTemplateRequestArgumentCaptor;
        private List<String> indexPatterns;

        @BeforeEach
        void setUp() {
            final OpenSearchTransport openSearchTransport = mock(OpenSearchTransport.class);
            when(openSearchClient._transport()).thenReturn(openSearchTransport);
            when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());

            putIndexTemplateRequestArgumentCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);

            indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
        }

        @Test
        void putTemplate_with_setTemplateName_performs_putIndexTemplate_request() throws IOException {
            final IndexTemplate indexTemplate = new ComposableIndexTemplate(new HashMap<>());
            indexTemplate.setTemplateName(indexTemplateName);
            objectUnderTest.putTemplate(indexTemplate);

            verify(openSearchIndicesClient).putIndexTemplate(putIndexTemplateRequestArgumentCaptor.capture());

            final PutIndexTemplateRequest actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

            assertThat(actualPutRequest.name(), equalTo(indexTemplateName));

            assertThat(actualPutRequest.version(), nullValue());
            assertThat(actualPutRequest.indexPatterns(), notNullValue());
            assertThat(actualPutRequest.indexPatterns(), equalTo(Collections.emptyList()));
            assertThat(actualPutRequest.template(), nullValue());
            assertThat(actualPutRequest.priority(), nullValue());
            assertThat(actualPutRequest.composedOf(), notNullValue());
            assertThat(actualPutRequest.composedOf(), equalTo(Collections.emptyList()));
        }

        @Test
        void putTemplate_with_setIndexPatterns_performs_putIndexTemplate_request() throws IOException {
            final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());

            final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
            final IndexTemplate indexTemplate = new ComposableIndexTemplate(new HashMap<>());
            indexTemplate.setTemplateName(indexTemplateName);
            indexTemplate.setIndexPatterns(indexPatterns);
            objectUnderTest.putTemplate(indexTemplate);

            verify(openSearchIndicesClient).putIndexTemplate(putIndexTemplateRequestArgumentCaptor.capture());

            final PutIndexTemplateRequest actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

            assertThat(actualPutRequest.name(), equalTo(indexTemplateName));
            assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));

            assertThat(actualPutRequest.version(), nullValue());
            assertThat(actualPutRequest.template(), nullValue());
            assertThat(actualPutRequest.priority(), nullValue());
            assertThat(actualPutRequest.composedOf(), notNullValue());
            assertThat(actualPutRequest.composedOf(), equalTo(Collections.emptyList()));
        }

        @Test
        void putTemplate_with_defined_template_values_performs_putIndexTemplate_request() throws IOException {
            final Long version = (long) (random.nextInt(10_000) + 100);
            final int priority = random.nextInt(1000) + 100;
            final String numberOfShards = Integer.toString(random.nextInt(1000) + 100);
            final List<String> composedOf = Collections.singletonList(UUID.randomUUID().toString());

            final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
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

            verify(openSearchIndicesClient).putIndexTemplate(putIndexTemplateRequestArgumentCaptor.capture());

            final PutIndexTemplateRequest actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();

            assertThat(actualPutRequest.name(), equalTo(indexTemplateName));
            assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
            assertThat(actualPutRequest.version(), equalTo(version));
            assertThat(actualPutRequest.priority(), equalTo(priority));
            assertThat(actualPutRequest.composedOf(), equalTo(composedOf));
            assertThat(actualPutRequest.template(), notNullValue());
            assertThat(actualPutRequest.template().mappings(), notNullValue());
            assertThat(actualPutRequest.template().mappings().dateDetection(), equalTo(true));
            assertThat(actualPutRequest.template().settings(), notNullValue());
            assertThat(actualPutRequest.template().settings().index(), notNullValue());
            assertThat(actualPutRequest.template().settings().index().numberOfShards(), equalTo(numberOfShards));
        }
    }
}