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
import org.opensearch.dataprepper.plugins.sink.opensearch.BackendVersion;
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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class V1TemplateStrategyTest {
    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;

    @Captor
    private ArgumentCaptor<JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse>> jsonEndpointArgumentCaptor;
    private Random random;
    private String templateName;

    @BeforeEach
    void setUp() {
        random = new Random();
        lenient().when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        templateName = UUID.randomUUID().toString();
    }

    private V1TemplateStrategy createObjectUnderTest() {
        return new V1TemplateStrategy(openSearchClient);
    }

    @Test
    void getExistingTemplateVersion_should_calls_existTemplate_with_templateName() throws IOException {
        final BooleanResponse booleanResponse = mock(BooleanResponse.class);
        when(booleanResponse.value()).thenReturn(false);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class)))
                .thenReturn(booleanResponse);

        createObjectUnderTest().getExistingTemplateVersion(templateName);

        final ArgumentCaptor<ExistsTemplateRequest> existsTemplateRequestArgumentCaptor = ArgumentCaptor.forClass(ExistsTemplateRequest.class);
        verify(openSearchIndicesClient).existsTemplate(existsTemplateRequestArgumentCaptor.capture());

        final ExistsTemplateRequest actualRequest = existsTemplateRequestArgumentCaptor.getValue();
        assertThat(actualRequest.name(), notNullValue());
        assertAll(
                () -> assertThat(actualRequest.name().size(), equalTo(1)),
                () -> assertThat(actualRequest.name(), hasItem(templateName))
        );
    }

    @Test
    void getExistingTemplateVersion_should_return_empty_if_no_template_exists() throws IOException {
        final BooleanResponse booleanResponse = mock(BooleanResponse.class);
        when(booleanResponse.value()).thenReturn(false);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class)))
                .thenReturn(booleanResponse);

        final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(templateName);

        assertThat(optionalVersion, notNullValue());
        assertThat(optionalVersion.isPresent(), equalTo(false));
    }

    @Nested
    class WithExistingTemplate {
        @BeforeEach
        void setUp() throws IOException {
            final BooleanResponse booleanResponse = mock(BooleanResponse.class);
            when(booleanResponse.value()).thenReturn(true);
            when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class)))
                    .thenReturn(booleanResponse);
        }

        @Test
        void getExistingTemplateVersion_should_return_empty_if_template_exists_without_version() throws IOException {
            final GetTemplateResponse getTemplateResponse = mock(GetTemplateResponse.class);
            final TemplateMapping templateMapping = mock(TemplateMapping.class);
            when(templateMapping.version()).thenReturn(null);
            when(getTemplateResponse.result()).thenReturn(Collections.singletonMap(templateName, templateMapping));
            when(openSearchIndicesClient.getTemplate(any(GetTemplateRequest.class)))
                    .thenReturn(getTemplateResponse);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(templateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(false));
        }

        @Test
        void getExistingTemplateVersion_should_return_template_version_if_template_exists() throws IOException {
            final GetTemplateResponse getTemplateResponse = mock(GetTemplateResponse.class);
            final TemplateMapping templateMapping = mock(TemplateMapping.class);
            final Long version = (long) (random.nextInt(10_000) + 100);
            when(templateMapping.version()).thenReturn(version);
            when(getTemplateResponse.result()).thenReturn(Collections.singletonMap(templateName, templateMapping));
            when(openSearchIndicesClient.getTemplate(any(GetTemplateRequest.class)))
                    .thenReturn(getTemplateResponse);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(templateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo(version));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, 2})
        void getExistingTemplateVersion_should_throw_if_get_template_returns_unexpected_number_of_templates(final int numberOfTemplatesReturned) throws IOException {
            final GetTemplateResponse getTemplateResponse = mock(GetTemplateResponse.class);
            final Map<String, TemplateMapping> templateResult = mock(Map.class);
            when(templateResult.size()).thenReturn(numberOfTemplatesReturned);
            when(getTemplateResponse.result()).thenReturn(templateResult);
            when(openSearchIndicesClient.getTemplate(any(GetTemplateRequest.class)))
                    .thenReturn(getTemplateResponse);

            final V1TemplateStrategy objectUnderTest = createObjectUnderTest();
            assertThrows(RuntimeException.class, () -> objectUnderTest.getExistingTemplateVersion(templateName));

            verify(openSearchIndicesClient).getTemplate(any(GetTemplateRequest.class));
        }
    }

    @Test
    void createTemplate_throws_if_template_is_not_LegacyIndexTemplate() {
        final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
        final IndexTemplate indexTemplate = mock(IndexTemplate.class);
        final V1TemplateStrategy objectUnderTest = createObjectUnderTest();

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.createTemplate(indexConfiguration,
                indexTemplate));
    }

    @Test
    void createTemplate_performs_putTemplate_request() throws IOException {
        final OpenSearchTransport openSearchTransport = mock(OpenSearchTransport.class);
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        final V1TemplateStrategy objectUnderTest = createObjectUnderTest();

        final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
        final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
        final IndexTemplate indexTemplate = objectUnderTest.createIndexTemplate(new HashMap<>());
        indexTemplate.setTemplateName(templateName);
        indexTemplate.setIndexPatterns(indexPatterns);
        objectUnderTest.createTemplate(indexConfiguration, indexTemplate);

        final ArgumentCaptor<PutTemplateRequest> putTemplateRequestArgumentCaptor = ArgumentCaptor.forClass(PutTemplateRequest.class);
        verify(openSearchIndicesClient).putTemplate(putTemplateRequestArgumentCaptor.capture());

        final PutTemplateRequest actualPutRequest = putTemplateRequestArgumentCaptor.getValue();

        assertThat(actualPutRequest.name(), equalTo(templateName));
        assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
    }

    @Test
    void createTemplate_es6() throws IOException {
        final OpenSearchTransport openSearchTransport = mock(OpenSearchTransport.class);
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        final OpenSearchTransport openSearchTransport1 = mock(OpenSearchTransport.class);
        when(openSearchIndicesClient._transport()).thenReturn(openSearchTransport1);
        final TransportOptions transportOptions = mock(TransportOptions.class);
        when(openSearchIndicesClient._transportOptions()).thenReturn(transportOptions);
        final V1TemplateStrategy objectUnderTest = createObjectUnderTest();

        final IndexConfiguration indexConfiguration = mock(IndexConfiguration.class);
        when(indexConfiguration.getBackendVersion()).thenReturn(BackendVersion.ES6);
        final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
        final IndexTemplate indexTemplate = objectUnderTest.createIndexTemplate(new HashMap<>());
        indexTemplate.setTemplateName(templateName);
        indexTemplate.setIndexPatterns(indexPatterns);
        objectUnderTest.createTemplate(indexConfiguration, indexTemplate);

        final ArgumentCaptor<PutTemplateRequest> putTemplateRequestArgumentCaptor = ArgumentCaptor.forClass(PutTemplateRequest.class);
        verify(openSearchTransport1).performRequest(
                putTemplateRequestArgumentCaptor.capture(), jsonEndpointArgumentCaptor.capture(), eq(transportOptions));

        final PutTemplateRequest actualPutRequest = putTemplateRequestArgumentCaptor.getValue();

        assertThat(actualPutRequest.name(), equalTo(templateName));
        assertThat(actualPutRequest.indexPatterns(), equalTo(indexPatterns));
        final JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse> endpoint = jsonEndpointArgumentCaptor.getValue();
        assertThat(endpoint.requestUrl(actualPutRequest), equalTo(
                String.format("/_template/%s?include_type_name=false", templateName)));
    }

    @Nested
    class IndexTemplateTests {
        private Map<String, Object> providedTemplateMap;

        @BeforeEach
        void setUp() {
            providedTemplateMap = new HashMap<>();
        }

        @Test
        void createIndexTemplate_setTemplateName_sets_the_name() {
            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));

            indexTemplate.setTemplateName(templateName);

            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
            assertThat(returnedTemplateMap, hasKey("name"));
            assertThat(returnedTemplateMap.get("name"), equalTo(templateName));

            assertThat(providedTemplateMap, not(hasKey("name")));
        }

        @Test
        void createIndexTemplate_setIndexPatterns_sets_the_indexPatterns() {
            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));

            final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
            indexTemplate.setIndexPatterns(indexPatterns);

            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
            assertThat(returnedTemplateMap, hasKey("index_patterns"));
            assertThat(returnedTemplateMap.get("index_patterns"), equalTo(indexPatterns));

            assertThat(providedTemplateMap, not(hasKey("index_patterns")));
        }

        @Test
        void putCustomSetting_setIndexPatterns_sets_existing_settings() {
            final String existingKey = UUID.randomUUID().toString();
            final String existingValue = UUID.randomUUID().toString();
            final Map<String, Object> providedSettings = Collections.singletonMap(existingKey, existingValue);
            providedTemplateMap.put("settings", providedSettings);
            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));

            final String customKey = UUID.randomUUID().toString();
            final String customValue = UUID.randomUUID().toString();

            indexTemplate.putCustomSetting(customKey, customValue);

            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
            assertThat(returnedTemplateMap, hasKey("settings"));
            assertThat(returnedTemplateMap.get("settings"), instanceOf(Map.class));
            final Map<String, Object> settings = (Map<String, Object>) returnedTemplateMap.get("settings");
            assertThat(settings, hasKey(customKey));
            assertThat(settings.get(customKey), equalTo(customValue));
            assertThat(settings, hasKey(existingKey));
            assertThat(settings.get(existingKey), equalTo(existingValue));

            assertThat(providedSettings, not(hasKey(customKey)));
        }

        @Test
        void putCustomSetting_setIndexPatterns_sets_new_settings() {
            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            assertThat(indexTemplate, instanceOf(V1TemplateStrategy.LegacyIndexTemplate.class));

            final String customKey = UUID.randomUUID().toString();
            final String customValue = UUID.randomUUID().toString();
            indexTemplate.putCustomSetting(customKey, customValue);

            final Map<String, Object> returnedTemplateMap = ((V1TemplateStrategy.LegacyIndexTemplate) indexTemplate).getTemplateMap();
            assertThat(returnedTemplateMap, hasKey("settings"));
            assertThat(returnedTemplateMap.get("settings"), instanceOf(Map.class));
            final Map<String, Object> settings = (Map<String, Object>) returnedTemplateMap.get("settings");
            assertThat(settings, hasKey(customKey));
            assertThat(settings.get(customKey), equalTo(customValue));

            assertThat(providedTemplateMap, not(hasKey("settings")));
        }

        @Test
        void getVersion_returns_empty_if_no_version() {
            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            final Optional<Long> optionalVersion = indexTemplate.getVersion();
            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(false));
        }

        @Test
        void getVersion_returns_version_from_root_map() {
            final Long version = (long) (random.nextInt(10_000) + 100);
            providedTemplateMap.put("version", version);

            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            final Optional<Long> optionalVersion = indexTemplate.getVersion();
            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo(version));
        }

        @Test
        void getVersion_returns_version_from_root_map_when_provided_as_int() {
            final Integer version = random.nextInt(10_000) + 100;
            providedTemplateMap.put("version", version);

            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            final Optional<Long> optionalVersion = indexTemplate.getVersion();
            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo((long) version));
        }
    }
}