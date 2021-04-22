package com.amazon.dataprepper.model.configuration;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class PluginSettingsTests {
    private static final String TEST_PLUGIN_NAME = "test";

    private static final String TEST_STRING_DEFAULT_VALUE = "DEFAULT";
    private static final String TEST_STRING_VALUE = "TEST";

    private static final int TEST_INT_DEFAULT_VALUE = 1000;
    private static final int TEST_INT_VALUE = TEST_INT_DEFAULT_VALUE + 1;

    private static final boolean TEST_BOOL_DEFAULT_VALUE = Boolean.FALSE;
    private static final boolean TEST_BOOL_VALUE = !TEST_BOOL_DEFAULT_VALUE;

    private static final long TEST_LONG_DEFAULT_VALUE = 1000L;
    private static final long TEST_LONG_VALUE = TEST_LONG_DEFAULT_VALUE + 1;

    private static final String TEST_INT_ATTRIBUTE = "int-attribute";
    private static final String TEST_STRING_ATTRIBUTE = "string-attribute";
    private static final String TEST_BOOL_ATTRIBUTE = "bool-attribute";
    private static final String TEST_LONG_ATTRIBUTE = "long-attribute";
    private static final String NOT_PRESENT_ATTRIBUTE = "not-present";


    @Test
    public void testPluginSetting() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, ImmutableMap.of());

        assertThat(pluginSetting, notNullValue());
    }

    @Test
    public void testPluginSetting_Name() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, ImmutableMap.of());

        assertThat(pluginSetting.getName(), is(TEST_PLUGIN_NAME));
    }

    @Test
    public void testPluginSetting_PipelineName() {
        final String TEST_PIPELINE = "test-pipeline";
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, ImmutableMap.of());
        pluginSetting.setPipelineName(TEST_PIPELINE);

        assertThat(pluginSetting.getPipelineName(), is(TEST_PIPELINE));
    }

    @Test
    public void testPluginSetting_NumberOfProcessWorkers() {
        final int TEST_WORKERS = 1;
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, ImmutableMap.of());
        pluginSetting.setProcessWorkers(TEST_WORKERS);

        assertThat(pluginSetting.getNumberOfProcessWorkers(), is(TEST_WORKERS));
    }

    @Test
    public void testGetAttributeFromSettings() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_INT_ATTRIBUTE, TEST_INT_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getAttributeFromSettings(TEST_INT_ATTRIBUTE), is(TEST_INT_VALUE));
    }

    @Test
    public void testGetAttributeOrDefault() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_INT_ATTRIBUTE, TEST_INT_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getAttributeOrDefault(TEST_INT_ATTRIBUTE, TEST_INT_DEFAULT_VALUE), is(TEST_INT_VALUE));
    }

    @Test
    public void testGetStringOrDefault() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_STRING_ATTRIBUTE, TEST_STRING_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getStringOrDefault(TEST_STRING_ATTRIBUTE, TEST_STRING_DEFAULT_VALUE),
                is(equalTo(TEST_STRING_VALUE)));
    }

    @Test
    public void testGetBooleanOrDefault() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_BOOL_ATTRIBUTE, TEST_BOOL_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getBooleanOrDefault(TEST_BOOL_ATTRIBUTE, TEST_BOOL_DEFAULT_VALUE), is(equalTo(TEST_BOOL_VALUE)));
    }

    @Test
    public void testGetLongOrDefault() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_LONG_ATTRIBUTE, TEST_LONG_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getLongOrDefault(TEST_LONG_ATTRIBUTE, TEST_LONG_DEFAULT_VALUE), is(equalTo(TEST_LONG_VALUE)));
    }

    @Test
    public void testGetIntegerOrDefault_AsString() {
        final String TEST_INT_VALUE_STRING = String.valueOf(TEST_INT_VALUE);
        final String TEST_INT_STRING_ATTRIBUTE = "int-string-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_STRINGS = ImmutableMap.of(TEST_INT_STRING_ATTRIBUTE, TEST_INT_VALUE_STRING);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_STRINGS);

        assertThat(pluginSetting.getIntegerOrDefault(TEST_INT_STRING_ATTRIBUTE, TEST_INT_DEFAULT_VALUE), is(TEST_INT_VALUE));
    }

    @Test
    public void testGetBooleanOrDefault_AsString() {
        final String TEST_BOOL_VALUE_STRING = String.valueOf(TEST_BOOL_VALUE);
        final String TEST_BOOL_STRING_ATTRIBUTE = "bool-string-attribute";

        final Map<String, Object> TEST_SETTINGS_AS_STRINGS = ImmutableMap.of(TEST_BOOL_STRING_ATTRIBUTE, TEST_BOOL_VALUE_STRING);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_STRINGS);

        assertThat(pluginSetting.getBooleanOrDefault(TEST_BOOL_STRING_ATTRIBUTE, TEST_BOOL_DEFAULT_VALUE), is(equalTo(TEST_BOOL_VALUE)));
    }

    @Test
    public void testGetLongOrDefault_AsString() {
        final String TEST_LONG_VALUE_STRING = String.valueOf(TEST_LONG_VALUE);
        final String TEST_LONG_STRING_ATTRIBUTE = "long-string-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_STRINGS = ImmutableMap.of(TEST_LONG_STRING_ATTRIBUTE, TEST_LONG_VALUE_STRING);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_STRINGS);

        assertThat(pluginSetting.getLongOrDefault(TEST_LONG_STRING_ATTRIBUTE, TEST_LONG_DEFAULT_VALUE), is(equalTo(TEST_LONG_VALUE)));
    }

    /**
     * Request attributes are present with null values, expect nulls to be returned
     */
    @Test
    public void testGetIntegerOrDefault_AsNull() {
        final String TEST_INT_NULL_ATTRIBUTE = "int-null-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_NULL = new HashMap<>();
        TEST_SETTINGS_AS_NULL.put(TEST_INT_NULL_ATTRIBUTE, null);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_NULL);

        // test attributes that exist when passing in a different default value
        assertThat(pluginSetting.getIntegerOrDefault(TEST_INT_NULL_ATTRIBUTE, TEST_INT_DEFAULT_VALUE), nullValue());
    }

    /**
     * Request attributes are present with null values, expect nulls to be returned
     */
    @Test
    public void testGetStringOrDefault_AsNull() {
        final String TEST_STRING_NULL_ATTRIBUTE = "string-null-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_NULL = new HashMap<>();
        TEST_SETTINGS_AS_NULL.put(TEST_STRING_NULL_ATTRIBUTE, null);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_NULL);

        // test attributes that exist when passing in a different default value
        assertThat(pluginSetting.getStringOrDefault(TEST_STRING_NULL_ATTRIBUTE, TEST_STRING_DEFAULT_VALUE), nullValue());
    }

    /**
     * Request attributes are present with null values, expect nulls to be returned
     */
    @Test
    public void testGetBooleanOrDefault_AsNull() {
        final String TEST_BOOL_NULL_ATTRIBUTE = "bool-null-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_NULL = new HashMap<>();
        TEST_SETTINGS_AS_NULL.put(TEST_BOOL_NULL_ATTRIBUTE, null);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_NULL);

        // test attributes that exist when passing in a different default value
        assertThat(pluginSetting.getBooleanOrDefault(TEST_BOOL_NULL_ATTRIBUTE, TEST_BOOL_DEFAULT_VALUE), nullValue());
    }

    /**
     * Request attributes are present with null values, expect nulls to be returned
     */
    @Test
    public void testGetLongOrDefault_AsNull() {
        final String TEST_LONG_NULL_ATTRIBUTE = "long-null-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_NULL = new HashMap<>();
        TEST_SETTINGS_AS_NULL.put(TEST_LONG_NULL_ATTRIBUTE, null);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_NULL);

        // test attributes that exist when passing in a different default value
        assertThat(pluginSetting.getLongOrDefault(TEST_LONG_NULL_ATTRIBUTE, TEST_LONG_DEFAULT_VALUE), nullValue());
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetSettings_Null() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getSettings(), nullValue());
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetAttributeFromSettings_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getAttributeFromSettings(NOT_PRESENT_ATTRIBUTE), nullValue());
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetAttributeOrDefault_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getAttributeOrDefault(NOT_PRESENT_ATTRIBUTE, TEST_INT_DEFAULT_VALUE), is(TEST_INT_DEFAULT_VALUE));
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetIntegerOrDefault_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getIntegerOrDefault(NOT_PRESENT_ATTRIBUTE, TEST_INT_DEFAULT_VALUE), is(TEST_INT_DEFAULT_VALUE));
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetStringOrDefault_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getStringOrDefault(NOT_PRESENT_ATTRIBUTE, TEST_STRING_DEFAULT_VALUE),
                is(equalTo(TEST_STRING_DEFAULT_VALUE)));
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetBooleanOrDefault_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getBooleanOrDefault(NOT_PRESENT_ATTRIBUTE, TEST_BOOL_DEFAULT_VALUE), is(equalTo(TEST_BOOL_DEFAULT_VALUE)));
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetLongOrDefault_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getLongOrDefault(NOT_PRESENT_ATTRIBUTE, TEST_LONG_DEFAULT_VALUE), is(equalTo(TEST_LONG_DEFAULT_VALUE)));
    }

    @Test
    public void testGetIntegerOrDefault_UnsupportedType() {
        final Object UNSUPPORTED_TYPE = new ArrayList<>();
        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_INT_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        // test attributes that exist when passing in a different default value
        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getIntegerOrDefault(TEST_INT_ATTRIBUTE, TEST_INT_DEFAULT_VALUE));
    }

    @Test
    public void testGetStringOrDefault_UnsupportedType() {
        final Object UNSUPPORTED_TYPE = new ArrayList<>();
        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRING_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getStringOrDefault(TEST_STRING_ATTRIBUTE, TEST_STRING_DEFAULT_VALUE));
    }

    @Test
    public void testGetBooleanOrDefault_UnsupportedType() {
        final Object UNSUPPORTED_TYPE = new ArrayList<>();
        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_BOOL_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getBooleanOrDefault(TEST_BOOL_ATTRIBUTE, TEST_BOOL_DEFAULT_VALUE));
    }

    @Test
    public void testGetLongOrDefault_UnsupportedType() {
        final Object UNSUPPORTED_TYPE = new ArrayList<>();
        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_LONG_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getLongOrDefault(TEST_LONG_ATTRIBUTE, TEST_LONG_DEFAULT_VALUE));
    }
}
