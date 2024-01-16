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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.indices.GetIndexTemplateResponse;
import org.opensearch.client.opensearch.indices.get_index_template.IndexTemplate;
import org.opensearch.client.opensearch.indices.get_index_template.IndexTemplateItem;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ComposableIndexTemplate.INDEX_SETTINGS_KEY;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ComposableIndexTemplate.TEMPLATE_KEY;

@ExtendWith(MockitoExtension.class)
class ComposableIndexTemplateStrategyTest {
    @Mock
    private IndexTemplateAPIWrapper<GetIndexTemplateResponse> indexTemplateAPIWrapper;
    @Mock
    private GetIndexTemplateResponse getIndexTemplateResponse;
    @Mock
    private IndexTemplateItem indexTemplateItem;
    @Mock
    private IndexTemplate indexTemplate;
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
        when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getIndexTemplateResponse));
        when(getIndexTemplateResponse.indexTemplates()).thenReturn(List.of(indexTemplateItem));
        when(indexTemplateItem.indexTemplate()).thenReturn(indexTemplate);
        createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);

        verify(indexTemplateAPIWrapper).getTemplate(eq(indexTemplateName));
        verify(indexTemplate).version();
    }

    @Test
    void getExistingTemplateVersion_should_return_empty_if_no_template_exists() throws IOException {
        final Optional<Long> version = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);
        assertThat(version.isEmpty(), is(true));
    }

    @Nested
    class WithExistingIndexTemplate {
        @Test
        void getExistingTemplateVersion_should_return_empty_if_index_template_exists_without_version() throws IOException {
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getIndexTemplateResponse));
            when(getIndexTemplateResponse.indexTemplates()).thenReturn(List.of(indexTemplateItem));
            when(indexTemplateItem.indexTemplate()).thenReturn(indexTemplate);
            when(indexTemplate.version()).thenReturn(null);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(false));
        }

        @Test
        void getExistingTemplateVersion_should_return_template_version_if_template_exists() throws IOException {
            final Long version = (long) (random.nextInt(10_000) + 100);
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getIndexTemplateResponse));
            when(getIndexTemplateResponse.indexTemplates()).thenReturn(List.of(indexTemplateItem));
            when(indexTemplateItem.indexTemplate()).thenReturn(indexTemplate);
            when(indexTemplate.version()).thenReturn(version);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo(version));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, 2})
        void getExistingTemplateVersion_should_throw_if_get_template_returns_unexpected_number_of_templates(
                final int numberOfTemplatesReturned) throws IOException {
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getIndexTemplateResponse));
            final List<IndexTemplateItem> indexTemplateItems = mock(List.class);
            when(indexTemplateItems.size()).thenReturn(numberOfTemplatesReturned);
            when(getIndexTemplateResponse.indexTemplates()).thenReturn(indexTemplateItems);

            assertThrows(RuntimeException.class,
                    () -> createObjectUnderTest().getExistingTemplateVersion(indexTemplateName));

            verify(indexTemplateAPIWrapper).getTemplate(eq(indexTemplateName));
        }
    }

    @Test
    void createTemplate_throws_if_putTemplate_throws() throws IOException {
        doThrow(IOException.class).when(indexTemplateAPIWrapper).putTemplate(any());
        final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate = mock(
                org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate.class);
        final ComposableIndexTemplateStrategy objectUnderTest = createObjectUnderTest();

        assertThrows(IOException.class, () -> objectUnderTest.createTemplate(indexTemplate));
    }

    @Test
    void createTemplate_performs_putTemplate_request() throws IOException {
        final ComposableIndexTemplateStrategy objectUnderTest = createObjectUnderTest();
        final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate = mock(org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate.class);

        objectUnderTest.createTemplate(indexTemplate);
        verify(indexTemplateAPIWrapper).putTemplate(indexTemplate);
    }

    @Nested
    class IndexTemplateTests {
        private Map<String, Object> providedTemplateMap;

        @BeforeEach
        void setUp() {
            providedTemplateMap = new HashMap<>();
        }

        @Test
        void getVersion_returns_empty_if_no_version() {
            final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate =
                    createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            final Optional<Long> optionalVersion = indexTemplate.getVersion();
            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(false));
        }

        @Test
        void getVersion_returns_version_from_root_map() {
            final Long version = (long) (random.nextInt(10_000) + 100);
            providedTemplateMap.put("version", version);

            final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate =
                    createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            final Optional<Long> optionalVersion = indexTemplate.getVersion();
            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo(version));
        }

        @Test
        void getVersion_returns_version_from_root_map_when_provided_as_int() {
            final Integer version = random.nextInt(10_000) + 100;
            providedTemplateMap.put("version", version);

            final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate =
                    createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            final Optional<Long> optionalVersion = indexTemplate.getVersion();
            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo((long) version));
        }

        @Test
        void putCustomSetting_with_no_existing_template_adds_template_and_settings() {
            final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate =
                    createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            String customKey = UUID.randomUUID().toString();
            String customValue = UUID.randomUUID().toString();

            indexTemplate.putCustomSetting(customKey, customValue);

            Map<String, Object> indexTemplateMap = ((ComposableIndexTemplate) indexTemplate).getIndexTemplateMap();

            assertThat(indexTemplateMap, hasKey(TEMPLATE_KEY));
            assertThat(indexTemplateMap.get(TEMPLATE_KEY), instanceOf(Map.class));
            Map<String, Object> templateMap = (Map<String, Object>) indexTemplateMap.get(TEMPLATE_KEY);
            assertThat(templateMap, hasKey(INDEX_SETTINGS_KEY));
            assertThat(templateMap.get(INDEX_SETTINGS_KEY), instanceOf(Map.class));
            Map<String, Object> settingsMap = (Map<String, Object>) templateMap.get(INDEX_SETTINGS_KEY);
            assertThat(settingsMap, hasKey(customKey));
            assertThat(settingsMap.get(customKey), equalTo(customValue));
        }

        @Test
        void putCustomSetting_with_existing_template_adds_settings_to_that_template() {
            String existingKey = UUID.randomUUID().toString();
            String existingValue = UUID.randomUUID().toString();
            Map<String, Object> template = new HashMap<>();
            template.put(existingKey, existingValue);
            providedTemplateMap.put(TEMPLATE_KEY, template);

            final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate =
                    createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            String customKey = UUID.randomUUID().toString();
            String customValue = UUID.randomUUID().toString();

            indexTemplate.putCustomSetting(customKey, customValue);

            Map<String, Object> indexTemplateMap = ((ComposableIndexTemplate) indexTemplate).getIndexTemplateMap();

            assertThat(indexTemplateMap, hasKey(TEMPLATE_KEY));
            assertThat(indexTemplateMap.get(TEMPLATE_KEY), instanceOf(Map.class));
            Map<String, Object> templateMap = (Map<String, Object>) indexTemplateMap.get(TEMPLATE_KEY);
            assertThat(templateMap, hasKey(INDEX_SETTINGS_KEY));
            assertThat(templateMap, hasKey(existingKey));
            assertThat(templateMap.get(existingKey), equalTo(existingValue));
            assertThat(templateMap.get(INDEX_SETTINGS_KEY), instanceOf(Map.class));
            Map<String, Object> settingsMap = (Map<String, Object>) templateMap.get(INDEX_SETTINGS_KEY);
            assertThat(settingsMap, hasKey(customKey));
            assertThat(settingsMap.get(customKey), equalTo(customValue));
        }

        @Test
        void putCustomSetting_with_existing_template_and_settings_puts_settings_to_that_settings() {
            String existingKey = UUID.randomUUID().toString();
            String existingValue = UUID.randomUUID().toString();
            Map<String, Object> template = new HashMap<>();
            HashMap<Object, Object> existingSettings = new HashMap<>();
            existingSettings.put(existingKey, existingValue);
            template.put(INDEX_SETTINGS_KEY, existingSettings);

            providedTemplateMap.put(TEMPLATE_KEY, template);

            final org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexTemplate indexTemplate =
                    createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            String customKey = UUID.randomUUID().toString();
            String customValue = UUID.randomUUID().toString();

            indexTemplate.putCustomSetting(customKey, customValue);

            Map<String, Object> indexTemplateMap = ((ComposableIndexTemplate) indexTemplate).getIndexTemplateMap();

            assertThat(indexTemplateMap, hasKey(TEMPLATE_KEY));
            assertThat(indexTemplateMap.get(TEMPLATE_KEY), instanceOf(Map.class));
            Map<String, Object> templateMap = (Map<String, Object>) indexTemplateMap.get(TEMPLATE_KEY);
            assertThat(templateMap, hasKey(INDEX_SETTINGS_KEY));
            assertThat(templateMap.get(INDEX_SETTINGS_KEY), instanceOf(Map.class));
            Map<String, Object> settingsMap = (Map<String, Object>) templateMap.get(INDEX_SETTINGS_KEY);
            assertThat(settingsMap, hasKey(customKey));
            assertThat(settingsMap.get(customKey), equalTo(customValue));
            assertThat(settingsMap, hasKey(existingKey));
            assertThat(settingsMap.get(existingKey), equalTo(existingValue));
        }
    }
}