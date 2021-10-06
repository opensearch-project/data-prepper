package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.plugins.TestSink;
import com.amazon.dataprepper.plugins.TestSinkUpdated;
import com.amazon.dataprepper.plugins.TestSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

class ClasspathPluginProviderTest {

    private Reflections reflections;

    @BeforeEach
    void setUp() {
        reflections = mock(Reflections.class);
    }

    private ClasspathPluginProvider createObjectUnderTest() {
        return new ClasspathPluginProvider(reflections);
    }

    @Test
    void findPlugin_should_scan_for_plugins() {
        final ClasspathPluginProvider objectUnderTest = createObjectUnderTest();

        then(reflections).shouldHaveNoInteractions();

        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(Collections.emptySet());

        objectUnderTest.findPluginClass(Sink.class, UUID.randomUUID().toString());

        then(reflections)
                .should()
                .getTypesAnnotatedWith(DataPrepperPlugin.class);
    }

    @Test
    void findPlugin_should_scan_for_plugins_only_once() {
        final ClasspathPluginProvider objectUnderTest = createObjectUnderTest();

        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(Collections.emptySet());

        for (int i = 0; i < 10; i++)
            objectUnderTest.findPluginClass(Sink.class, UUID.randomUUID().toString());

        then(reflections)
                .should()
                .getTypesAnnotatedWith(DataPrepperPlugin.class);
    }

    @Test
    void findPlugin_should_return_null_if_no_plugins_found() {
        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(Collections.emptySet());

        assertThat(createObjectUnderTest().findPluginClass(Sink.class, "test_sink"),
            nullValue());
    }

    @Nested
    class WithPredefinedPlugins {

        @BeforeEach
        void setUp() {
            given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                    .willReturn(new HashSet<>(Arrays.asList(
                            TestSink.class, TestSource.class, TestSinkUpdated.class)));
        }

        @Test
        void findPlugin_should_return_null_if_no_plugin_for_the_name() {
            assertThat(createObjectUnderTest().findPluginClass(Sink.class, UUID.randomUUID().toString()),
                    nullValue());
        }

        @Test
        void findPlugin_should_return_null_if_plugin_found_for_another_type() {
            assertThat(createObjectUnderTest().findPluginClass(Source.class, "test_sink"),
                    nullValue());
        }

        @Test
        void findPlugin_should_return_plugin_if_found_for_name_and_type() {
            assertThat(createObjectUnderTest().findPluginClass(Sink.class, "test_sink"),
                    equalTo(TestSink.class));
        }

        @Test
        void findPlugin_should_return_plugin_if_found_for_name_and_type_using_pluginType() {
            assertThat(createObjectUnderTest().findPluginClass(Sink.class, "test_sink_updated"),
                    equalTo(TestSinkUpdated.class));
        }
    }
}