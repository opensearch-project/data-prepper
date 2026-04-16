/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.lang.reflect.Field;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)

class FilterListProcessorConfigTest {

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Test
    void test_default_target_returns_source_when_target_not_set() throws NoSuchFieldException, IllegalAccessException {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        setField(config, "iterateOn", "my_source");
        assertThat(config.getTarget(), is(equalTo("my_source")));
    }

    @Test
    void test_target_returns_explicit_target_when_set() throws NoSuchFieldException, IllegalAccessException {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        setField(config, "iterateOn", "my_source");
        setField(config, "target", "my_target");
        assertThat(config.getTarget(), is(equalTo("my_target")));
    }

    @Test
    void test_default_filter_list_when_is_null() {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        assertThat(config.getFilterListWhen(), is(nullValue()));
    }

    @Test
    void test_default_tags_on_failure_is_null() {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        assertThat(config.getTagsOnFailure(), is(nullValue()));
    }

    @Test
    void test_getters_return_set_values() throws NoSuchFieldException, IllegalAccessException {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();

        setField(config, "iterateOn", "my_source");
        setField(config, "target", "my_target");
        setField(config, "keepElementWhen", "/type == \"cve\"");
        setField(config, "filterListWhen", "/enabled == true");
        setField(config, "tagsOnFailure", List.of("tag1"));

        assertThat(config.getIterateOn(), is(equalTo("my_source")));
        assertThat(config.getTarget(), is(equalTo("my_target")));
        assertThat(config.getKeepElementWhen(), is(equalTo("/type == \"cve\"")));
        assertThat(config.getFilterListWhen(), is(equalTo("/enabled == true")));
        assertThat(config.getTagsOnFailure(), is(equalTo(List.of("tag1"))));
    }

    @Test
    void test_validateExpressions_throws_when_keep_when_is_invalid() throws NoSuchFieldException, IllegalAccessException {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        setField(config, "keepElementWhen", "invalid");
        when(expressionEvaluator.isValidExpressionStatement("invalid")).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, () -> config.validateExpressions(expressionEvaluator));
    }

    @Test
    void test_validateExpressions_throws_when_filter_list_when_is_invalid() throws NoSuchFieldException, IllegalAccessException {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        setField(config, "keepElementWhen", "/type == \"cve\"");
        setField(config, "filterListWhen", "invalid");
        when(expressionEvaluator.isValidExpressionStatement("invalid")).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, () -> config.validateExpressions(expressionEvaluator));
    }

    @Test
    void test_validateExpressions_does_not_throw_when_expressions_are_valid() throws NoSuchFieldException, IllegalAccessException {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        setField(config, "keepElementWhen", "/type == \"cve\"");
        setField(config, "filterListWhen", "/enabled == true");
        when(expressionEvaluator.isValidExpressionStatement("/type == \"cve\"")).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement("/enabled == true")).thenReturn(true);

        assertDoesNotThrow(() -> config.validateExpressions(expressionEvaluator));
    }

    private void setField(final Object object, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, value);
    }
}
