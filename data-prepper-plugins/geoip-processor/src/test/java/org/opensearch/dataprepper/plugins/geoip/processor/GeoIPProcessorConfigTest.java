/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorConfigTest {

    private GeoIPProcessorConfig geoIPProcessorConfig;

    @BeforeEach
    void setUp() {
        geoIPProcessorConfig = new GeoIPProcessorConfig();
    }

    @Test
    void testDefaultConfig() {
        assertThat(geoIPProcessorConfig.getEntries(), equalTo(null));
        assertThat(geoIPProcessorConfig.getTagsOnEngineFailure(), equalTo(null));
        assertThat(geoIPProcessorConfig.getTagsOnIPNotFound(), equalTo(null));
        assertThat(geoIPProcessorConfig.getWhenCondition(), equalTo(null));
    }

    @Test
    void testGetEntries() throws NoSuchFieldException, IllegalAccessException {
        final List<EntryConfig> entries = List.of(new EntryConfig());
        final List<String> tagsOnEngineFailure = List.of("tag1", "tag2");
        final List<String> tagsOnIPNotFound = List.of("tag3");
        final String whenCondition = "/ip == 1.2.3.4";

        ReflectivelySetField.setField(GeoIPProcessorConfig.class, geoIPProcessorConfig, "entries", entries);
        ReflectivelySetField.setField(GeoIPProcessorConfig.class, geoIPProcessorConfig, "tagsOnEngineFailure", tagsOnEngineFailure);
        ReflectivelySetField.setField(GeoIPProcessorConfig.class, geoIPProcessorConfig, "tagsOnIPNotFound", tagsOnIPNotFound);
        ReflectivelySetField.setField(GeoIPProcessorConfig.class, geoIPProcessorConfig, "whenCondition", whenCondition);

        assertThat(geoIPProcessorConfig.getEntries(), equalTo(entries));
        assertThat(geoIPProcessorConfig.getTagsOnEngineFailure(), equalTo(tagsOnEngineFailure));
        assertThat(geoIPProcessorConfig.getTagsOnIPNotFound(), equalTo(tagsOnIPNotFound));
        assertThat(geoIPProcessorConfig.getWhenCondition(), equalTo(whenCondition));
    }
}
