/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.indices.get_index_template.IndexTemplate;
import org.opensearch.client.opensearch.indices.get_index_template.IndexTemplateItem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ComposableIndexTemplateStrategyTest {
    @Mock
    private IndexTemplateAPIWrapper<IndexTemplateItem> indexTemplateAPIWrapper;
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
        @Test
        void getExistingTemplateVersion_should_return_empty_if_index_template_exists_without_version() throws IOException {
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(indexTemplateItem));
            when(indexTemplateItem.indexTemplate()).thenReturn(indexTemplate);
            when(indexTemplate.version()).thenReturn(null);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(false));
        }

        @Test
        void getExistingTemplateVersion_should_return_template_version_if_template_exists() throws IOException {
            final Long version = (long) (random.nextInt(10_000) + 100);
            when(indexTemplateAPIWrapper.getTemplate(any())).thenReturn(Optional.of(indexTemplateItem));
            when(indexTemplateItem.indexTemplate()).thenReturn(indexTemplate);
            when(indexTemplate.version()).thenReturn(version);

            final Optional<Long> optionalVersion = createObjectUnderTest().getExistingTemplateVersion(indexTemplateName);

            assertThat(optionalVersion, notNullValue());
            assertThat(optionalVersion.isPresent(), equalTo(true));
            assertThat(optionalVersion.get(), equalTo(version));
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
    }
}