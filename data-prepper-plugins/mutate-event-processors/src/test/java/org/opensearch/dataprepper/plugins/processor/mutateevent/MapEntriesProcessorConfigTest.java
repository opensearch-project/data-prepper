/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class MapEntriesProcessorConfigTest {

    @Test
    void defaults_are_correct() {
        final MapEntriesProcessorConfig config = new MapEntriesProcessorConfig();
        assertThat(config.getTarget(), nullValue());
        assertThat(config.getExcludeNullEmptyValues(), equalTo(false));
        assertThat(config.getAppendIfTargetExists(), equalTo(false));
        assertThat(config.getMapEntriesWhen(), nullValue());
        assertThat(config.getTagsOnFailure(), nullValue());
    }

    @Test
    void getEffectiveTarget_returns_target_when_set() throws Exception {
        final MapEntriesProcessorConfig config = new MapEntriesProcessorConfig();
        setField(config, "source", "/names");
        setField(config, "target", "/agents");
        assertThat(config.getEffectiveTarget(), equalTo("/agents"));
    }

    @Test
    void getEffectiveTarget_returns_source_when_target_is_null() throws Exception {
        final MapEntriesProcessorConfig config = new MapEntriesProcessorConfig();
        setField(config, "source", "/names");
        assertThat(config.getEffectiveTarget(), equalTo("/names"));
    }

    private void setField(final Object obj, final String fieldName, final Object value) throws Exception {
        final Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }
}
