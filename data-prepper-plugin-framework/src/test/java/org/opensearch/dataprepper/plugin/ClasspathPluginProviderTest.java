/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.test.TestSink;
import org.opensearch.dataprepper.plugins.test.TestSource;
import org.reflections.Reflections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_DEPRECATED_NAME;

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
    void findPlugins_should_scan_for_plugins() {
        final ClasspathPluginProvider objectUnderTest = createObjectUnderTest();

        then(reflections).shouldHaveNoInteractions();

        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(Collections.emptySet());

        objectUnderTest.findPluginClasses(Sink.class);

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
    void findPlugins_should_scan_for_plugins_only_once() {
        final ClasspathPluginProvider objectUnderTest = createObjectUnderTest();

        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(Collections.emptySet());

        for (int i = 0; i < 10; i++)
            objectUnderTest.findPluginClasses(Sink.class);

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

    @Test
    void findPlugin_should_return_empty_for_default_deprecated_name() {
        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(new HashSet<>(List.of(TestSource.class)));

        final Optional<Class<? extends Source>> optionalPlugin = createObjectUnderTest().findPluginClass(Source.class, DEFAULT_DEPRECATED_NAME);
        assertThat(optionalPlugin, notNullValue());
        assertThat(optionalPlugin.isPresent(), equalTo(false));
    }

    @Test
    void findPlugin_should_return_plugin_if_found_for_deprecated_name_and_type_using_pluginType() {
        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(new HashSet<>(List.of(TestSource.class)));

        final Optional<Class<? extends Source>> optionalPlugin = createObjectUnderTest().findPluginClass(Source.class, "test_source_deprecated_name");
        assertThat(optionalPlugin, notNullValue());
        assertThat(optionalPlugin.isPresent(), equalTo(true));
        assertThat(optionalPlugin.get(), equalTo(TestSource.class));
    }

    @Test
    void findPlugin_should_return_empty_for_default_alternate_name() {
        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(new HashSet<>(List.of(TestSource.class)));

        final Optional<Class<? extends Source>> optionalPlugin = createObjectUnderTest()
                .findPluginClass(Source.class, UUID.randomUUID().toString());
        assertThat(optionalPlugin, notNullValue());
        assertThat(optionalPlugin.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"test_source_alternate_name1", "test_source_alternate_name2"})
    void findPlugin_should_return_plugin_if_found_for_alternate_name_and_type_using_pluginType(final String alternateSourceName) {
        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(new HashSet<>(List.of(TestSource.class)));

        final Optional<Class<? extends Source>> optionalPlugin = createObjectUnderTest().findPluginClass(Source.class, alternateSourceName);
        assertThat(optionalPlugin, notNullValue());
        assertThat(optionalPlugin.isPresent(), equalTo(true));
        assertThat(optionalPlugin.get(), equalTo(TestSource.class));
    }

    @Test
    void findPlugins_should_return_empty_if_no_plugins_found() {
        given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                .willReturn(Collections.emptySet());

        final Collection<Class<? extends PluginSetting>> foundPlugins = createObjectUnderTest().findPluginClasses(
                PluginSetting.class);
        assertThat(foundPlugins, notNullValue());
        assertThat(foundPlugins.isEmpty(), is(true));
    }

    @Nested
    class WithPredefinedPlugins {

        @BeforeEach
        void setUp() {
            given(reflections.getTypesAnnotatedWith(DataPrepperPlugin.class))
                    .willReturn(new HashSet<>(Arrays.asList(
                            TestSink.class, TestSource.class)));
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
        void findPlugin_should_return_plugin_if_found_for_name_and_type_using_pluginType() {
            final Optional<Class<? extends Sink>> optionalPlugin = createObjectUnderTest().findPluginClass(Sink.class, "test_sink");
            assertThat(optionalPlugin, notNullValue());
            assertThat(optionalPlugin.isPresent(), equalTo(true));
            assertThat(optionalPlugin.get(), equalTo(TestSink.class));
        }

        @Test
        void findPlugins_should_return_plugins_if_plugin_found_for_specified_type() {
            final Set<Class<? extends Source>> foundPlugins = createObjectUnderTest().findPluginClasses(Source.class);
            assertThat(foundPlugins, notNullValue());
            assertThat(foundPlugins.size(), equalTo(1));
            assertThat(foundPlugins.stream().iterator().next(), equalTo(TestSource.class));
        }
    }
}