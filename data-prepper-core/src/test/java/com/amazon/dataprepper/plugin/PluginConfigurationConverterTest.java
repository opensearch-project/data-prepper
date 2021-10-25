package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

class PluginConfigurationConverterTest {
    private PluginSetting pluginSetting;

    static class TestConfiguration {
        private String myValue;

        public String getMyValue() {
            return myValue;
        }
    }

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);
    }

    private PluginConfigurationConverter createObjectUnderTest() {
        return new PluginConfigurationConverter();
    }

    @Test
    void convert_with_null_configurationType_should_throw() {
        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class,
                () -> objectUnderTest.convert(null, pluginSetting));
    }

    @Test
    void convert_with_null_pluginSetting_should_throw() {
        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class,
                () -> objectUnderTest.convert(PluginSetting.class, null));
    }

    @Test
    void convert_with_PluginSetting_target_should_return_pluginSetting_object_directly() {
        assertThat(createObjectUnderTest().convert(PluginSetting.class, pluginSetting),
                sameInstance(pluginSetting));

        then(pluginSetting).shouldHaveNoInteractions();
    }

    @Test
    void convert_with_other_target_should_return_pluginSetting_object_directly() {

        final String value = UUID.randomUUID().toString();
        given(pluginSetting.getSettings())
                .willReturn(Collections.singletonMap("my_value", value));

        final Object convertedConfiguration = createObjectUnderTest().convert(TestConfiguration.class, pluginSetting);

        assertThat(convertedConfiguration, notNullValue());
        assertThat(convertedConfiguration, instanceOf(TestConfiguration.class));

        final TestConfiguration convertedTestConfiguration = (TestConfiguration) convertedConfiguration;

        assertThat(convertedTestConfiguration.getMyValue(), equalTo(value));
    }

}