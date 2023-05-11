/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
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

    private static final List<String> TEST_STRINGLIST_VALUE = new ArrayList<>();

    private static final Map<String, String> TEST_STRINGMAP_VALUE = new HashMap<>();

    private static final Map<String, List<String>> TEST_STRINGLISTMAP_VALUE = new HashMap<>();

    private static final String TEST_INT_ATTRIBUTE = "int-attribute";
    private static final String TEST_STRING_ATTRIBUTE = "string-attribute";
    private static final String TEST_STRINGLIST_ATTRIBUTE = "list-attribute";
    private static final String TEST_STRINGMAP_ATTRIBUTE = "map-attribute";
    private static final String TEST_STRINGLISTMAP_ATTRIBUTE = "list-map-attribute";
    private static final String TEST_BOOL_ATTRIBUTE = "bool-attribute";
    private static final String TEST_LONG_ATTRIBUTE = "long-attribute";
    private static final String NOT_PRESENT_ATTRIBUTE = "not-present";

    @Before
    public void setup() {
        TEST_STRINGLIST_VALUE.add("value1");
        TEST_STRINGLIST_VALUE.add("value2");
        TEST_STRINGLIST_VALUE.add("value3");

        TEST_STRINGMAP_VALUE.put("key1", "value1");
        TEST_STRINGMAP_VALUE.put("key2", "value2");
        TEST_STRINGMAP_VALUE.put("key3", "value3");

        final int NUM_LISTS = 3;

        for (int i = 0; i < NUM_LISTS; i++) {
            final List<String> TEST_STRINGLISTMAP_VALUE_LIST = new ArrayList<>();
            TEST_STRINGLISTMAP_VALUE_LIST.add("value_1" + String.valueOf(i));
            TEST_STRINGLISTMAP_VALUE_LIST.add("value_2" + String.valueOf(i));
            TEST_STRINGLISTMAP_VALUE_LIST.add("value_3" + String.valueOf(i));

            TEST_STRINGLISTMAP_VALUE.put("key_" + String.valueOf(i), TEST_STRINGLISTMAP_VALUE_LIST);
        }

    }

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
    public void testGetTypedList() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_STRINGLIST_ATTRIBUTE, TEST_STRINGLIST_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getTypedList(TEST_STRINGLIST_ATTRIBUTE, String.class), is(equalTo(TEST_STRINGLIST_VALUE)));
    }

    @Test
    public void testGetTypedMap() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_STRINGMAP_ATTRIBUTE, TEST_STRINGMAP_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getTypedMap(TEST_STRINGMAP_ATTRIBUTE, String.class, String.class), is(equalTo(TEST_STRINGMAP_VALUE)));
    }

    @Test
    public void testGetTypedListMap() {
        final Map<String, Object> TEST_SETTINGS = ImmutableMap.of(TEST_STRINGLISTMAP_ATTRIBUTE, TEST_STRINGLISTMAP_VALUE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS);

        assertThat(pluginSetting.getTypedListMap(TEST_STRINGLISTMAP_ATTRIBUTE, String.class, String.class), is(equalTo(TEST_STRINGLISTMAP_VALUE)));
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
    public void testGetTypedList_AsNull() {
        final String TEST_STRINGLIST_NULL_ATTRIBUTE = "typedlist-null-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_NULL = new HashMap<>();
        TEST_SETTINGS_AS_NULL.put(TEST_STRINGLIST_NULL_ATTRIBUTE, null);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_NULL);

        assertThat(pluginSetting.getTypedList(TEST_STRINGLIST_NULL_ATTRIBUTE, String.class), nullValue());
    }

    /**
     * Request attributes are present with null values, expect nulls to be returned
     */
    @Test
    public void testGetTypedMap_AsNull() {
        final String TEST_STRINGMAP_NULL_ATTRIBUTE = "typedgmap-null-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_NULL = new HashMap<>();
        TEST_SETTINGS_AS_NULL.put(TEST_STRINGMAP_NULL_ATTRIBUTE, null);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_NULL);

        assertThat(pluginSetting.getTypedMap(TEST_STRINGMAP_NULL_ATTRIBUTE, String.class, String.class), nullValue());
    }

    /**
     * Request attributes are present with null values, expect nulls to be returned
     */
    @Test
    public void testGetTypedListMap_AsNull() {
        final String TEST_STRINGLISTMAP_NULL_ATTRIBUTE = "typedlistmap-null-attribute";
        final Map<String, Object> TEST_SETTINGS_AS_NULL = new HashMap<>();
        TEST_SETTINGS_AS_NULL.put(TEST_STRINGLISTMAP_NULL_ATTRIBUTE, null);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_AS_NULL);

        assertThat(pluginSetting.getTypedListMap(TEST_STRINGLISTMAP_NULL_ATTRIBUTE, String.class, String.class), nullValue());
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
    public void testGetTypedList_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getTypedList(NOT_PRESENT_ATTRIBUTE, String.class),
                is(equalTo(Collections.emptyList())));
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetStringMapOrDefault_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getTypedMap(NOT_PRESENT_ATTRIBUTE, String.class, String.class),
                is(equalTo(Collections.emptyMap())));
    }

    /**
     * Requested attributes are not present, expect default values to be returned
     */
    @Test
    public void testGetTypedListMap_NotPresent() {
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, null);

        assertThat(pluginSetting.getTypedListMap(NOT_PRESENT_ATTRIBUTE, String.class, String.class),
                is(equalTo(Collections.emptyMap())));
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
    public void testGetTypedList_UnsupportedType() {
        final String UNSUPPORTED_TYPE = "not-stringlist";
        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGLIST_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedList(TEST_STRINGLIST_ATTRIBUTE, String.class));
    }

    @Test
    public void testGetTypedList_UnsupportedListType() {
        final List<Integer> UNSUPPORTED_TYPE = new ArrayList<>();
        UNSUPPORTED_TYPE.add(1);
        UNSUPPORTED_TYPE.add(2);
        UNSUPPORTED_TYPE.add(3);

        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGLIST_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedList(TEST_STRINGLIST_ATTRIBUTE, String.class));
    }

    @Test
    public void testGetTypedMap_UnsupportedType() {
        final String UNSUPPORTED_TYPE = "not-stringmap";
        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGMAP_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedMap(TEST_STRINGMAP_ATTRIBUTE, String.class, String.class));
    }


    @Test
    public void testGetTypedMap_UnsupportedMapValueType() {
        final Map<String, Integer> UNSUPPORTED_TYPE = new HashMap<>();
        UNSUPPORTED_TYPE.put("key1", 1);
        UNSUPPORTED_TYPE.put("key2", 2);
        UNSUPPORTED_TYPE.put("key3", 3);

        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGMAP_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedMap(TEST_STRINGMAP_ATTRIBUTE, String.class, String.class));
    }

    @Test
    public void testGetTypedListMap_UnsupportedType() {
        final String UNSUPPORTED_TYPE = "not-stringmap";
        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGLISTMAP_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedListMap(TEST_STRINGLISTMAP_ATTRIBUTE, String.class, String.class));
    }

    @Test
    public void testGetTypedListMap_UnsupportedMapValueType() {
        final Map<String, String> UNSUPPORTED_TYPE = new HashMap<>();
        UNSUPPORTED_TYPE.put("key1", "value1");
        UNSUPPORTED_TYPE.put("key2", "value2");
        UNSUPPORTED_TYPE.put("key3", "value3");

        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGLISTMAP_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedListMap(TEST_STRINGLISTMAP_ATTRIBUTE, String.class, String.class));
    }

    @Test
    public void testGetTypedListMap_UnsupportedMapKeyType() {
        final Map<Integer, List<String>> UNSUPPORTED_TYPE = new HashMap<>();
        final List<String> STRING_LIST_VALUE = new ArrayList<>();
        STRING_LIST_VALUE.add("value1");

        UNSUPPORTED_TYPE.put(1, STRING_LIST_VALUE);
        UNSUPPORTED_TYPE.put(2, STRING_LIST_VALUE);
        UNSUPPORTED_TYPE.put(3, STRING_LIST_VALUE);

        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGLISTMAP_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedListMap(TEST_STRINGLISTMAP_ATTRIBUTE, String.class, String.class));
    }

    @Test
    public void testGetTypedListMap_UnsupportedMapValueListType() {
        final Map<String, List<Integer>> UNSUPPORTED_TYPE = new HashMap<>();
        final List<Integer> INT_LIST_VALUE = new ArrayList<>();
        INT_LIST_VALUE.add(1);

        UNSUPPORTED_TYPE.put("value1", INT_LIST_VALUE);
        UNSUPPORTED_TYPE.put("value2", INT_LIST_VALUE);
        UNSUPPORTED_TYPE.put("value3", INT_LIST_VALUE);

        final Map<String, Object> TEST_SETTINGS_WITH_UNSUPPORTED_TYPE = ImmutableMap.of(TEST_STRINGLISTMAP_ATTRIBUTE, UNSUPPORTED_TYPE);
        final PluginSetting pluginSetting = new PluginSetting(TEST_PLUGIN_NAME, TEST_SETTINGS_WITH_UNSUPPORTED_TYPE);

        assertThrows(IllegalArgumentException.class, () -> pluginSetting.getTypedListMap(TEST_STRINGLISTMAP_ATTRIBUTE, String.class, String.class));
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
