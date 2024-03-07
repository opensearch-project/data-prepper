/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.sink.SinkContext;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class SinkContextPluginSettingTest {
    private String name;
    private Map<String, Object> settings;
    private SinkContext sinkContext;

    @BeforeEach
    void setUp() {
        name = UUID.randomUUID().toString();
        settings = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        sinkContext = mock(SinkContext.class);

    }

    private SinkContextPluginSetting createObjectUnderTest() {
        return new SinkContextPluginSetting(name, settings, sinkContext);
    }

    @Test
    void getName_returns_name_from_constructor() {
        assertThat(createObjectUnderTest().getName(), equalTo(name));
    }

    @Test
    void getSettings_returns_settings_from_constructor() {
        assertThat(createObjectUnderTest().getSettings(), equalTo(settings));
    }

    @Test
    void getRoutes_returns_sink_context_from_constructor() {
        assertThat(createObjectUnderTest().getSinkContext(), equalTo(sinkContext));
    }
}
