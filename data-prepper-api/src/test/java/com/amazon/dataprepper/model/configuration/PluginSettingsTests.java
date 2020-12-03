package com.amazon.dataprepper.model.configuration;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class PluginSettingsTests {
    private static final String TEST_PLUGIN_NAME = "test";
    private static final String TEST_STRING_VALUE = "TEST";
    private static final int TEST_INT_VALUE = 1000;
    private static final boolean TEST_BOOL_VALUE = Boolean.TRUE;
    private static final long TEST_LONG_VALUE = 1000L;
    private static final String TEST_ATTRIBUTE_1 = "attribute1";
    private static final String TEST_ATTRIBUTE_2 = "attribute2";
    private static final String TEST_ATTRIBUTE_3 = "attribute3";
    private static final String TEST_ATTRIBUTE_4 = "attribute4";
    private static final String NOT_PRESENT_ATTRIBUTE = "not-present";
    private static final String TEST_PIPELINE = "test-pipeline";
    private static final int TEST_WORKERS = 1;
    private static final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_ATTRIBUTE_1, TEST_INT_VALUE,
            TEST_ATTRIBUTE_2, TEST_STRING_VALUE,
            TEST_ATTRIBUTE_3, TEST_BOOL_VALUE,
            TEST_ATTRIBUTE_4, TEST_LONG_VALUE);

    @Test
    public void testPluginSettingConstructorGetterSetters() {
        final PluginSetting pluginSettings = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);
        pluginSettings.setPipelineName(TEST_PIPELINE);
        pluginSettings.setProcessWorkers(TEST_WORKERS);
        assertThat(pluginSettings, notNullValue());
        assertThat(pluginSettings.getName(), is(TEST_PLUGIN_NAME));
        assertThat(pluginSettings.getSettings(), is(TEST_SETTINGS));
        assertThat(pluginSettings.getPipelineName(), is(TEST_PIPELINE));
        assertThat(pluginSettings.getNumberOfProcessWorkers(), is(TEST_WORKERS));

        assertThat(pluginSettings.getAttributeFromSettings(TEST_ATTRIBUTE_1), is(TEST_INT_VALUE));
        assertThat(pluginSettings.getAttributeOrDefault(TEST_ATTRIBUTE_1, 500), is(TEST_INT_VALUE));
        assertThat(pluginSettings.getIntegerOrDefault(TEST_ATTRIBUTE_1, 500), is(TEST_INT_VALUE));
        assertThat(pluginSettings.getStringOrDefault(TEST_ATTRIBUTE_2, NOT_PRESENT_ATTRIBUTE),
                is(equalTo(TEST_STRING_VALUE)));
        assertThat(pluginSettings.getBooleanOrDefault(TEST_ATTRIBUTE_3, Boolean.FALSE), is(equalTo(TEST_BOOL_VALUE)));
        assertThat(pluginSettings.getLongOrDefault(TEST_ATTRIBUTE_4, Long.MAX_VALUE), is(equalTo(TEST_LONG_VALUE)));

        assertThat(pluginSettings.getAttributeFromSettings(NOT_PRESENT_ATTRIBUTE), nullValue());
        assertThat(pluginSettings.getAttributeOrDefault(NOT_PRESENT_ATTRIBUTE, 500), is(500));
        assertThat(pluginSettings.getIntegerOrDefault(NOT_PRESENT_ATTRIBUTE, 500), is(500));
        assertThat(pluginSettings.getStringOrDefault(NOT_PRESENT_ATTRIBUTE, NOT_PRESENT_ATTRIBUTE),
                is(equalTo(NOT_PRESENT_ATTRIBUTE)));
        assertThat(pluginSettings.getBooleanOrDefault(NOT_PRESENT_ATTRIBUTE, Boolean.FALSE), is(equalTo(Boolean.FALSE)));
        assertThat(pluginSettings.getLongOrDefault(NOT_PRESENT_ATTRIBUTE, Long.MAX_VALUE), is(equalTo(Long.MAX_VALUE)));
    }

    @Test
    public void testNullSettingsInPluginSetting() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);
        pluginSetting.setPipelineName(TEST_PIPELINE);
        pluginSetting.setProcessWorkers(TEST_WORKERS);

        assertThat(pluginSetting, notNullValue());
        assertThat(pluginSetting.getName(), is(TEST_PLUGIN_NAME));
        assertThat(pluginSetting.getSettings(), nullValue());
        assertThat(pluginSetting.getPipelineName(), is(TEST_PIPELINE));
        assertThat(pluginSetting.getNumberOfProcessWorkers(), is(TEST_WORKERS));

        assertThat(pluginSetting.getAttributeFromSettings(NOT_PRESENT_ATTRIBUTE), nullValue());
        assertThat(pluginSetting.getAttributeOrDefault(NOT_PRESENT_ATTRIBUTE, 500), is(500));
        assertThat(pluginSetting.getIntegerOrDefault(NOT_PRESENT_ATTRIBUTE, 500), is(500));
        assertThat(pluginSetting.getStringOrDefault(NOT_PRESENT_ATTRIBUTE, NOT_PRESENT_ATTRIBUTE),
                is(equalTo(NOT_PRESENT_ATTRIBUTE)));
        assertThat(pluginSetting.getBooleanOrDefault(NOT_PRESENT_ATTRIBUTE, Boolean.FALSE), is(equalTo(Boolean.FALSE)));
        assertThat(pluginSetting.getLongOrDefault(NOT_PRESENT_ATTRIBUTE, Long.MAX_VALUE), is(equalTo(Long.MAX_VALUE)));
    }
}
