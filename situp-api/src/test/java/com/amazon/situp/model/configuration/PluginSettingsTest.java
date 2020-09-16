package com.amazon.situp.model.configuration;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class PluginSettingsTest {
    private static final String TEST_PLUGIN_NAME = "test";
    private static final Map<String, Object> TEST_SETTINGS = ImmutableMap.of("attribute1", 1000,
            "attribute2", 2000);

    private PluginSetting pluginSetting;

    @Before
    public void setup() {
        pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);
    }
    @Test
    public void testPluginSettingsCreation() {
        assertThat(pluginSetting, notNullValue());
    }

    @Test
    public void testPluginSettingsNameAndAttribute() {
        assertThat(pluginSetting.getName(), is(TEST_PLUGIN_NAME));
        assertThat(pluginSetting.getSettings(), is(TEST_SETTINGS));
        assertThat(pluginSetting.getAttributeFromSettings("attribute1"), is(1000));
        assertThat(pluginSetting.getAttributeOrDefault("not-present", 500), is(500));

        assertThat(pluginSetting.getAttributeFromSettings("not-present"), nullValue());
    }
}
