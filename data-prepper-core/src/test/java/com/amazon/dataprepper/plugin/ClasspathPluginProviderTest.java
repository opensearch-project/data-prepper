package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.plugins.TestSink;
import com.amazon.dataprepper.plugins.TestSinkDeprecatedApproach;
import com.amazon.dataprepper.plugins.TestSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
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
    void findPlugin_should_return_empty_if_no_plugins_found() {
        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(Collections.emptySet());

        final Optional<Class<? extends Sink>> optionalPlugin = createObjectUnderTest().findPluginClass(Sink.class, "test_sink");
        assertThat(optionalPlugin, notNullValue());
        assertThat(optionalPlugin.isPresent(), equalTo(false));
    }

    @Nested
    class WithPredefinedPlugins {

        @BeforeEach
        void setUp() {
            given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                    .willReturn(new HashSet<>(Arrays.asList(
                            TestSink.class, TestSource.class, TestSinkDeprecatedApproach.class)));
        }

        @Test
        void findPlugin_should_return_empty_if_no_plugin_for_the_name() {
            final Optional<Class<? extends Sink>> optionalPlugin = createObjectUnderTest().findPluginClass(Sink.class, UUID.randomUUID().toString());
            assertThat(optionalPlugin, notNullValue());
            assertThat(optionalPlugin.isPresent(), equalTo(false));
        }

        @Test
        void findPlugin_should_return_empty_if_plugin_found_for_another_type() {
            final Optional<Class<? extends Source>> optionalPlugin = createObjectUnderTest().findPluginClass(Source.class, "test_sink");
            assertThat(optionalPlugin, notNullValue());
            assertThat(optionalPlugin.isPresent(), equalTo(false));
        }

        @Test
        void findPlugin_should_return_plugin_if_found_for_name_and_type() {
            final Optional<Class<? extends Sink>> optionalPlugin = createObjectUnderTest().findPluginClass(Sink.class, "test_sink_deprecated_type");
            assertThat(optionalPlugin, notNullValue());
            assertThat(optionalPlugin.isPresent(), equalTo(true));
            assertThat(optionalPlugin.get(), equalTo(TestSinkDeprecatedApproach.class));
        }

        @Test
        void findPlugin_should_return_plugin_if_found_for_name_and_type_using_pluginType() {
            final Optional<Class<? extends Sink>> optionalPlugin = createObjectUnderTest().findPluginClass(Sink.class, "test_sink");
            assertThat(optionalPlugin, notNullValue());
            assertThat(optionalPlugin.isPresent(), equalTo(true));
            assertThat(optionalPlugin.get(), equalTo(TestSink.class));
        }
    }
}