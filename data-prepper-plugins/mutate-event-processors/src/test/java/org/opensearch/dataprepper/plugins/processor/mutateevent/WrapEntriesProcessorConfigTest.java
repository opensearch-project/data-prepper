/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.lang.reflect.Field;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WrapEntriesProcessorConfigTest {

    @Test
    void defaults_are_correct() {
        final WrapEntriesProcessorConfig config = new WrapEntriesProcessorConfig();
        assertThat(config.getTarget(), nullValue());
        assertThat(config.getExcludeNullEmptyValues(), equalTo(false));
        assertThat(config.getAppendIfTargetExists(), equalTo(false));
        assertThat(config.getWrapEntriesWhen(), nullValue());
        assertThat(config.getTagsOnFailure(), nullValue());
    }

    @Test
    void getEffectiveTarget_returns_target_when_set() throws Exception {
        final WrapEntriesProcessorConfig config = new WrapEntriesProcessorConfig();
        setField(config, "source", "/names");
        setField(config, "target", "/agents");
        assertThat(config.getEffectiveTarget(), equalTo("/agents"));
    }

    @Test
    void getEffectiveTarget_returns_source_when_target_is_null() throws Exception {
        final WrapEntriesProcessorConfig config = new WrapEntriesProcessorConfig();
        setField(config, "source", "/names");
        assertThat(config.getEffectiveTarget(), equalTo("/names"));
    }

    @Test
    void validateExpressions_with_invalid_wrap_entries_when_throws_InvalidPluginConfigurationException() throws Exception {
        final WrapEntriesProcessorConfig config = new WrapEntriesProcessorConfig();
        final String condition = UUID.randomUUID().toString();
        final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);
        setField(config, "wrapEntriesWhen", condition);
        when(expressionEvaluator.isValidExpressionStatement(condition)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, () -> config.validateExpressions(expressionEvaluator));
    }

    @Test
    void validateExpressions_with_empty_target_throws_InvalidPluginConfigurationException() throws Exception {
        final WrapEntriesProcessorConfig config = new WrapEntriesProcessorConfig();
        final ExpressionEvaluator expressionEvaluator = mock(ExpressionEvaluator.class);
        setField(config, "target", "");

        assertThrows(InvalidPluginConfigurationException.class, () -> config.validateExpressions(expressionEvaluator));
    }

    private void setField(final Object obj, final String fieldName, final Object value) throws Exception {
        final Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }
}
