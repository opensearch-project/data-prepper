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
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.TemplateMapping;

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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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

@ExtendWith(MockitoExtension.class)
class V1TemplateStrategyTest {
    @Mock
    private IndexTemplateAPIWrapper<GetTemplateResponse> indexTemplateAPIWrapper;
    @Mock
    private GetTemplateResponse getTemplateResponse;
    @Mock
    private TemplateMapping templateMapping;
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
        when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getTemplateResponse));
        when(getTemplateResponse.result()).thenReturn(Map.of(templateName, templateMapping));
        createObjectUnderTest().getExistingTemplateVersion(templateName);

        verify(indexTemplateAPIWrapper).getTemplate(eq(templateName));
        verify(templateMapping).version();
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
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getTemplateResponse));
            when(getTemplateResponse.result()).thenReturn(Map.of(templateName, templateMapping));
            when(templateMapping.version()).thenReturn(null);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(templateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(false));
        }

        @Test
        void getExistingTemplateVersion_should_return_template_version_if_template_exists() throws IOException {
            final Long version = (long) (random.nextInt(10_000) + 100);
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getTemplateResponse));
            when(getTemplateResponse.result()).thenReturn(Map.of(templateName, templateMapping));
            when(templateMapping.version()).thenReturn(version);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(templateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo(version));
        }

        @ParameterizedTest
        @ValueSource(ints = {0, 2})
        void getExistingTemplateVersion_should_throw_if_get_template_returns_unexpected_number_of_templates(
                final int numberOfTemplatesReturned) throws IOException {
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(getTemplateResponse));
            final Map<String, TemplateMapping> templateResult = mock(Map.class);
            when(templateResult.size()).thenReturn(numberOfTemplatesReturned);
            when(getTemplateResponse.result()).thenReturn(templateResult);

            assertThrows(RuntimeException.class,
                    () -> createObjectUnderTest().getExistingTemplateVersion(templateName));

            verify(indexTemplateAPIWrapper).getTemplate(eq(templateName));
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

            assertThat(indexTemplate, instanceOf(LegacyIndexTemplate.class));

            indexTemplate.setTemplateName(templateName);

            final Map<String, Object> returnedTemplateMap = ((LegacyIndexTemplate) indexTemplate).getTemplateMap();
            assertThat(returnedTemplateMap, hasKey("name"));
            assertThat(returnedTemplateMap.get("name"), equalTo(templateName));

            assertThat(providedTemplateMap, not(hasKey("name")));
        }

        @Test
        void createIndexTemplate_setIndexPatterns_sets_the_indexPatterns() {
            final IndexTemplate indexTemplate = createObjectUnderTest().createIndexTemplate(providedTemplateMap);

            assertThat(indexTemplate, instanceOf(LegacyIndexTemplate.class));

            final List<String> indexPatterns = Collections.singletonList(UUID.randomUUID().toString());
            indexTemplate.setIndexPatterns(indexPatterns);

            final Map<String, Object> returnedTemplateMap = ((LegacyIndexTemplate) indexTemplate).getTemplateMap();
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

            assertThat(indexTemplate, instanceOf(LegacyIndexTemplate.class));

            final String customKey = UUID.randomUUID().toString();
            final String customValue = UUID.randomUUID().toString();

            indexTemplate.putCustomSetting(customKey, customValue);

            final Map<String, Object> returnedTemplateMap = ((LegacyIndexTemplate) indexTemplate).getTemplateMap();
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

            assertThat(indexTemplate, instanceOf(LegacyIndexTemplate.class));

            final String customKey = UUID.randomUUID().toString();
            final String customValue = UUID.randomUUID().toString();
            indexTemplate.putCustomSetting(customKey, customValue);

            final Map<String, Object> returnedTemplateMap = ((LegacyIndexTemplate) indexTemplate).getTemplateMap();
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