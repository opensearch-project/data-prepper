/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.TemplateMapping;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ComposableIndexTemplateStrategyTest {
    @Mock
    private IndexTemplateAPIWrapper<IndexTemplateItem> indexTemplateAPIWrapper;
    @Mock
    private IndexTemplateItem indexTemplateItem;
//    @Mock
//    private OpenSearchClient openSearchClient;
//
//    @Mock
//    private OpenSearchIndicesClient openSearchIndicesClient;
    private Random random;
    private String indexTemplateName;

    @BeforeEach
    void setUp() {
        random = new Random();
        indexTemplateName = UUID.randomUUID().toString();
    }

    private ComposableIndexTemplateStrategy createObjectUnderTest() {
        return new ComposableIndexTemplateStrategy(indexTemplateAPIWrapper);
    }

    @Test
    void getExistingTemplateVersion_should_calls_getTemplate_with_templateName() throws IOException {
        when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(indexTemplateItem));
        createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);

        verify(indexTemplateAPIWrapper).getTemplate(eq(indexTemplateName));
    }

    @Test
    void getExistingTemplateVersion_should_return_empty_if_no_template_exists() throws IOException {
        final Optional<Long> version = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);
        assertThat(version.isEmpty(), is(true));
    }

    @Nested
    class WithExistingIndexTemplate {
//        @BeforeEach
//        void setUp() throws IOException {
//            final BooleanResponse booleanResponse = mock(BooleanResponse.class);
//            when(booleanResponse.value()).thenReturn(true);
//            when(openSearchIndicesClient.existsIndexTemplate(any(ExistsIndexTemplateRequest.class)))
//                    .thenReturn(booleanResponse);
//        }
//
//        @Test
//        void getExistingTemplateVersion_should_return_empty_if_index_template_exists_without_version() throws IOException {
//            final GetIndexTemplateResponse getIndexTemplateResponse = mock(GetIndexTemplateResponse.class);
//            final IndexTemplateItem indexTemplateItem = mock(IndexTemplateItem.class);
//            org.opensearch.client.opensearch.indices.get_index_template.IndexTemplate indexTemplate = mock(org.opensearch.client.opensearch.indices.get_index_template.IndexTemplate.class);
//            when(indexTemplate.version()).thenReturn(null);
//            when(indexTemplateItem.indexTemplate()).thenReturn(indexTemplate);
//            when(getIndexTemplateResponse.indexTemplates()).thenReturn(Collections.singletonList(indexTemplateItem));
//            when(openSearchIndicesClient.getIndexTemplate(any(GetIndexTemplateRequest.class)))
//                    .thenReturn(getIndexTemplateResponse);
//
//            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);
//
//            assertThat(optionalVersion, notNullValue());
//            assertThat(optionalVersion.isPresent(), equalTo(false));
//        }
//
//        @Test
//        void getExistingTemplateVersion_should_return_template_version_if_template_exists() throws IOException {
//            final Long version = (long) (random.nextInt(10_000) + 100);
//            final GetIndexTemplateResponse getIndexTemplateResponse = mock(GetIndexTemplateResponse.class);
//            final IndexTemplateItem indexTemplateItem = mock(IndexTemplateItem.class);
//            org.opensearch.client.opensearch.indices.get_index_template.IndexTemplate indexTemplate = mock(org.opensearch.client.opensearch.indices.get_index_template.IndexTemplate.class);
//            when(indexTemplate.version()).thenReturn(version);
//            when(indexTemplateItem.indexTemplate()).thenReturn(indexTemplate);
//            when(getIndexTemplateResponse.indexTemplates()).thenReturn(Collections.singletonList(indexTemplateItem));
//            when(openSearchIndicesClient.getIndexTemplate(any(GetIndexTemplateRequest.class)))
//                    .thenReturn(getIndexTemplateResponse);
//
//            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);
//
//            assertThat(optionalVersion, notNullValue());
//            assertThat(optionalVersion.isPresent(), equalTo(true));
//            assertThat(optionalVersion.get(), equalTo(version));
//        }
//
//        @ParameterizedTest
//        @ValueSource(ints = {0, 2})
//        void getExistingTemplateVersion_should_throw_if_get_template_returns_unexpected_number_of_templates(final int numberOfTemplatesReturned) throws IOException {
//            final GetIndexTemplateResponse getIndexTemplateResponse = mock(GetIndexTemplateResponse.class);
//            final List<IndexTemplateItem> templateResult = mock(List.class);
//            when(templateResult.size()).thenReturn(numberOfTemplatesReturned);
//            when(getIndexTemplateResponse.indexTemplates()).thenReturn(templateResult);
//            when(openSearchIndicesClient.getIndexTemplate(any(GetIndexTemplateRequest.class)))
//                    .thenReturn(getIndexTemplateResponse);
//
//
//            final ComposableIndexTemplateStrategy objectUnderTest = createObjectUnderTest();
//            assertThrows(RuntimeException.class, () -> objectUnderTest.getExistingTemplateVersion(indexTemplateName));
//
//            verify(openSearchIndicesClient).getIndexTemplate(any(GetIndexTemplateRequest.class));
//        }
    }
//
//    @Test
//    void createTemplate_throws_if_template_is_not_ComposableIndexTemplate() {
//        final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
//        final IndexTemplate indexTemplate = mock(IndexTemplate.class);
//
//        final ComposableIndexTemplateStrategy objectUnderTest = createObjectUnderTest();
//
//        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.createTemplate(
//                indexConfiguration, indexTemplate));
//    }
//
//    @Nested
//    class IndexTemplateWithCreateTemplateTests {
//        private ArgumentCaptor<PutIndexTemplateRequest> putIndexTemplateRequestArgumentCaptor;
//        private List<String> indexPatterns;
//
//        @BeforeEach
//        void setUp() {
//            final OpenSearchTransport openSearchTransport = mock(OpenSearchTransport.class);
//            when(openSearchClient._transport()).thenReturn(openSearchTransport);
//            when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
//
//            putIndexTemplateRequestArgumentCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
//
//            indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
//        }
//
//        @Test
//        void createTemplate_with_setTemplateName_performs_putIndexTemplate_request() throws IOException {
//            final ComposableIndexTemplateStrategy objectUnderTest = createObjectUnderTest();
//
//            final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
//            final IndexTemplate indexTemplate = objectUnderTest.createIndexTemplate(new HashMap<>());
//            indexTemplate.setTemplateName(indexTemplateName);
//            objectUnderTest.createTemplate(indexConfiguration, indexTemplate);
//
//            verify(openSearchIndicesClient).putIndexTemplate(putIndexTemplateRequestArgumentCaptor.capture());
//
//            final PutIndexTemplateRequest actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();
//
//            assertThat(actualPutRequest.name(), equalTo(indexTemplateName));
//
//            assertThat(actualPutRequest.version(), nullValue());
//            assertThat(actualPutRequest.indexPatterns(), notNullValue());
//            assertThat(actualPutRequest.indexPatterns(), equalTo(Collections.emptyList()));
//            assertThat(actualPutRequest.template(), nullValue());
//            assertThat(actualPutRequest.priority(), nullValue());
//            assertThat(actualPutRequest.composedOf(), notNullValue());
//            assertThat(actualPutRequest.composedOf(), equalTo(Collections.emptyList()));
//        }
//
//        @Test
//        void createTemplate_with_setIndexPatterns_performs_putIndexTemplate_request() throws IOException {
//            final ComposableIndexTemplateStrategy objectUnderTest = createObjectUnderTest();
//
//            final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
//
//            final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
//            final IndexTemplate indexTemplate = objectUnderTest.createIndexTemplate(new HashMap<>());
//            indexTemplate.setTemplateName(indexTemplateName);
//            indexTemplate.setIndexPatterns(indexPatterns);
//            objectUnderTest.createTemplate(indexConfiguration, indexTemplate);
//
//            verify(openSearchIndicesClient).putIndexTemplate(putIndexTemplateRequestArgumentCaptor.capture());
//
//            final PutIndexTemplateRequest actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();
//
//            assertThat(actualPutRequest.name(), equalTo(indexTemplateName));
//            assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
//
//            assertThat(actualPutRequest.version(), nullValue());
//            assertThat(actualPutRequest.template(), nullValue());
//            assertThat(actualPutRequest.priority(), nullValue());
//            assertThat(actualPutRequest.composedOf(), notNullValue());
//            assertThat(actualPutRequest.composedOf(), equalTo(Collections.emptyList()));
//        }
//
//        @Test
//        void createTemplate_with_defined_template_values_performs_putIndexTemplate_request() throws IOException {
//            final ComposableIndexTemplateStrategy objectUnderTest = createObjectUnderTest();
//
//            final Long version = (long) (random.nextInt(10_000) + 100);
//            final int priority = random.nextInt(1000) + 100;
//            final String numberOfShards = Integer.toString(random.nextInt(1000) + 100);
//            final List<String> composedOf = Collections.singletonList(UUID.randomUUID().toString());
//
//            final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
//            final IndexTemplate indexTemplate = objectUnderTest.createIndexTemplate(
//                    Map.of("version", version,
//                            "priority", priority,
//                            "composed_of", composedOf,
//                            "template", Map.of(
//                                    "settings", Map.of(
//                                            "index", Map.of("number_of_shards", numberOfShards)),
//                                    "mappings", Map.of("date_detection", true)
//                            )
//                    ));
//            indexTemplate.setTemplateName(indexTemplateName);
//            indexTemplate.setIndexPatterns(indexPatterns);
//            objectUnderTest.createTemplate(indexConfiguration, indexTemplate);
//
//            verify(openSearchIndicesClient).putIndexTemplate(putIndexTemplateRequestArgumentCaptor.capture());
//
//            final PutIndexTemplateRequest actualPutRequest = putIndexTemplateRequestArgumentCaptor.getValue();
//
//            assertThat(actualPutRequest.name(), equalTo(indexTemplateName));
//            assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
//            assertThat(actualPutRequest.version(), equalTo(version));
//            assertThat(actualPutRequest.priority(), equalTo(priority));
//            assertThat(actualPutRequest.composedOf(), equalTo(composedOf));
//            assertThat(actualPutRequest.template(), notNullValue());
//            assertThat(actualPutRequest.template().mappings(), notNullValue());
//            assertThat(actualPutRequest.template().mappings().dateDetection(), equalTo(true));
//            assertThat(actualPutRequest.template().settings(), notNullValue());
//            assertThat(actualPutRequest.template().settings().index(), notNullValue());
//            assertThat(actualPutRequest.template().settings().index().numberOfShards(), equalTo(numberOfShards));
//        }
//    }
//
//    @Nested
//    class IndexTemplateTests {
//        private Map<String, Object> providedTemplateMap;
//
//        @BeforeEach
//        void setUp() {
//            providedTemplateMap = new HashMap<>();
//        }
//
//        @Test
//        void getVersion_returns_empty_if_no_version() {
//            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);
//
//            final Optional<Long> optionalVersion = indexTemplate.getVersion();
//            assertThat(optionalVersion, notNullValue());
//            assertThat(optionalVersion.isPresent(), equalTo(false));
//        }
//
//        @Test
//        void getVersion_returns_version_from_root_map() {
//            final Long version = (long) (random.nextInt(10_000) + 100);
//            providedTemplateMap.put("version", version);
//
//            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);
//
//            final Optional<Long> optionalVersion = indexTemplate.getVersion();
//            assertThat(optionalVersion, notNullValue());
//            assertThat(optionalVersion.isPresent(), equalTo(true));
//            assertThat(optionalVersion.get(), equalTo(version));
//        }
//
//        @Test
//        void getVersion_returns_version_from_root_map_when_provided_as_int() {
//            final Integer version = random.nextInt(10_000) + 100;
//            providedTemplateMap.put("version", version);
//
//            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);
//
//            final Optional<Long> optionalVersion = indexTemplate.getVersion();
//            assertThat(optionalVersion, notNullValue());
//            assertThat(optionalVersion.isPresent(), equalTo(true));
//            assertThat(optionalVersion.get(), equalTo((long) version));
//        }
//    }
}