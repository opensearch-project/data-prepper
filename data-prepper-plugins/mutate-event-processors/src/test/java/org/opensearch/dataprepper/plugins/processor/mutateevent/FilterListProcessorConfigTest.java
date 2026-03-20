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

import java.lang.reflect.Field;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class FilterListProcessorConfigTest {

    @Test
    void test_default_target_is_null() {
        final FilterListProcessorConfig config = new FilterListProcessorConfig();
        assertThat(config.getTarget(), is(nullValue()));
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

        setField(config, "source", "my_source");
        setField(config, "target", "my_target");
        setField(config, "keepWhen", "/type == \"cve\"");
        setField(config, "filterListWhen", "/enabled == true");
        setField(config, "tagsOnFailure", List.of("tag1"));

        assertThat(config.getSource(), is(equalTo("my_source")));
        assertThat(config.getTarget(), is(equalTo("my_target")));
        assertThat(config.getKeepWhen(), is(equalTo("/type == \"cve\"")));
        assertThat(config.getFilterListWhen(), is(equalTo("/enabled == true")));
        assertThat(config.getTagsOnFailure(), is(equalTo(List.of("tag1"))));
    }

    private void setField(final Object object, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, value);
    }
}
