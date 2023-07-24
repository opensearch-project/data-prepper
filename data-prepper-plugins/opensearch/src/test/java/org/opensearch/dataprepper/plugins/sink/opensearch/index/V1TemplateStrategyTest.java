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
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;
import org.opensearch.client.opensearch.indices.PutTemplateResponse;
import org.opensearch.client.opensearch.indices.TemplateMapping;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
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
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class V1TemplateStrategyTest {
    @Mock
    private IndexTemplateAPIWrapper<TemplateMapping> indexTemplateAPIWrapper;
    @Mock
    private TemplateMapping templateMapping;
//    @Mock
//    private OpenSearchClient openSearchClient;
//
//    @Mock
//    private OpenSearchIndicesClient openSearchIndicesClient;
//
//    @Captor
//    private ArgumentCaptor<JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse>> jsonEndpointArgumentCaptor;
    private Random random;
    private String templateName;

    @BeforeEach
    void setUp() {
        random = new Random();
        templateName = UUID.randomUUID().toString();
    }

    private V1TemplateStrategy createObjectUnderTest() {
        return new V1TemplateStrategy(indexTemplateAPIWrapper);
    }

    @Test
    void getExistingTemplateVersion_should_calls_getTemplate_with_templateName() throws IOException {
        when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(templateMapping));
        createObjectUnderTest().getExistingTemplateVersion(templateName);

        verify(indexTemplateAPIWrapper).getTemplate(eq(templateName));
    }

    @Test
    void getExistingTemplateVersion_should_return_empty_if_no_template_exists() throws IOException {
        final Optional<Long> version = createObjectUnderTest().getExistingTemplateVersion(templateName);
        assertThat(version.isEmpty(), is(true));
    }

    @Nested
    class WithExistingTemplate {
        @Test
        void getExistingTemplateVersion_should_return_empty_if_template_exists_without_version() throws IOException {
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(templateMapping));
            when(templateMapping.version()).thenReturn(null);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(templateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(false));
        }

        @Test
        void getExistingTemplateVersion_should_return_template_version_if_template_exists() throws IOException {
            final Long version = (long) (random.nextInt(10_000) + 100);
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(templateMapping));
            when(templateMapping.version()).thenReturn(version);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(templateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo(version));
        }
    }

    @Test
    void createTemplate_throws_if_putTemplate_throws() throws IOException {
        doThrow(IOException.class).when(indexTemplateAPIWrapper).putTemplate(any());
        final IndexTemplate indexTemplate = mock(IndexTemplate.class);
        final V1TemplateStrategy objectUnderTest = createObjectUnderTest();

        assertThrows(IOException.class, () -> objectUnderTest.createTemplate(indexTemplate));
    }

    @Test
    void createTemplate_performs_putTemplate_request() throws IOException {
        final V1TemplateStrategy objectUnderTest = createObjectUnderTest();
        final IndexTemplate indexTemplate = mock(IndexTemplate.class);

        objectUnderTest.createTemplate(indexTemplate);
        verify(indexTemplateAPIWrapper).putTemplate(indexTemplate);
    }
//
//    @Test
//    void createTemplate_es6() throws IOException {
//        final OpenSearchTransport openSearchTransport = mock(OpenSearchTransport.class);
//        when(openSearchClient._transport()).thenReturn(openSearchTransport);
//        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
//        final OpenSearchTransport openSearchTransport1 = mock(OpenSearchTransport.class);
//        when(openSearchIndicesClient._transport()).thenReturn(openSearchTransport1);
//        final TransportOptions transportOptions = mock(TransportOptions.class);
//        when(openSearchIndicesClient._transportOptions()).thenReturn(transportOptions);
//        final V1TemplateStrategy objectUnderTest = createObjectUnderTest();
//
//        final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
//        when(indexConfiguration.getDistributionVersion()).thenReturn(DistributionVersion.ES6);
//        final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
//        final IndexTemplate indexTemplate = objectUnderTest.createIndexTemplate(new HashMap<>());
//        indexTemplate.setTemplateName(templateName);
//        indexTemplate.setIndexPatterns(indexPatterns);
//        objectUnderTest.createTemplate(indexConfiguration, indexTemplate);
//
//        final ArgumentCaptor<PutTemplateRequest> putTemplateRequestArgumentCaptor = ArgumentCaptor.forClass(PutTemplateRequest.class);
//        verify(openSearchTransport1).performRequest(
//                putTemplateRequestArgumentCaptor.capture(), jsonEndpointArgumentCaptor.capture(), eq(transportOptions));
//
//        final PutTemplateRequest actualPutRequest = putTemplateRequestArgumentCaptor.getValue();
//
//        assertThat(actualPutRequest.name(), equalTo(templateName));
//        assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
//        final JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse> endpoint = jsonEndpointArgumentCaptor.getValue();
//        assertThat(endpoint.requestUrl(actualPutRequest), equalTo(
//                String.format("/_template/%s?include_type_name=false", templateName)));
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
//        void createIndexTemplate_setTemplateName_sets_the_name() {
//            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);
//
//            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));
//
//            indexTemplate.setTemplateName(templateName);
//
//            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
//            assertThat(returnedTemplateMap, hasKey("name"));
//            assertThat(returnedTemplateMap.get("name"), equalTo(templateName));
//
//            assertThat(providedTemplateMap, not(hasKey("name")));
//        }
//
//        @Test
//        void createIndexTemplate_setIndexPatterns_sets_the_indexPatterns() {
//            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);
//
//            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));
//
//            final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
//            indexTemplate.setIndexPatterns(indexPatterns);
//
//            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
//            assertThat(returnedTemplateMap, hasKey("index_patterns"));
//            assertThat(returnedTemplateMap.get("index_patterns"), equalTo(indexPatterns));
//
//            assertThat(providedTemplateMap, not(hasKey("index_patterns")));
//        }
//
//        @Test
//        void putCustomSetting_setIndexPatterns_sets_existing_settings() {
//            final String existingKey = UUID.randomUUID().toString();
//            final String existingValue = UUID.randomUUID().toString();
//            final Map<String, Object> providedSettings = Collections.singletonMap(existingKey, existingValue);
//            providedTemplateMap.put("settings", providedSettings);
//            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);
//
//            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));
//
//            final String customKey = UUID.randomUUID().toString();
//            final String customValue = UUID.randomUUID().toString();
//
//            indexTemplate.putCustomSetting(customKey, customValue);
//
//            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
//            assertThat(returnedTemplateMap, hasKey("settings"));
//            assertThat(returnedTemplateMap.get("settings"), instanceOf(Map.class));
//            final Map<String, Object> settings = (Map<String, Object>) returnedTemplateMap.get("settings");
//            assertThat(settings, hasKey(customKey));
//            assertThat(settings.get(customKey), equalTo(customValue));
//            assertThat(settings, hasKey(existingKey));
//            assertThat(settings.get(existingKey), equalTo(existingValue));
//
//            assertThat(providedSettings, not(hasKey(customKey)));
//        }
//
//        @Test
//        void putCustomSetting_setIndexPatterns_sets_new_settings() {
//            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);
//
//            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));
//
//            final String customKey = UUID.randomUUID().toString();
//            final String customValue = UUID.randomUUID().toString();
//            indexTemplate.putCustomSetting(customKey, customValue);
//
//            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
//            assertThat(returnedTemplateMap, hasKey("settings"));
//            assertThat(returnedTemplateMap.get("settings"), instanceOf(Map.class));
//            final Map<String, Object> settings = (Map<String, Object>) returnedTemplateMap.get("settings");
//            assertThat(settings, hasKey(customKey));
//            assertThat(settings.get(customKey), equalTo(customValue));
//
//            assertThat(providedTemplateMap, not(hasKey("settings")));
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