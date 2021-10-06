package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.plugins.TestSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class DefaultPluginFactoryTest {

    private PluginProviderLoader pluginProviderLoader;
    private PluginCreator pluginCreator;
    private Collection<PluginProvider> pluginProviders;
    private PluginProvider firstPluginProvider;
    private Class<?> baseClass;
    private String pluginName;

    @BeforeEach
    void setUp() {
        pluginProviderLoader = mock(PluginProviderLoader.class);
        pluginCreator = mock(PluginCreator.class);

        pluginProviders = new ArrayList<>();
        given(pluginProviderLoader.getPluginProviders()).willReturn(pluginProviders);
        firstPluginProvider = mock(PluginProvider.class);
        pluginProviders.add(firstPluginProvider);

        baseClass = Sink.class;
        pluginName = UUID.randomUUID().toString();
    }

    private DefaultPluginFactory createObjectUnderTest() {
        return new DefaultPluginFactory(pluginProviderLoader, pluginCreator);
    }

    @Test
    void constructor_should_throw_if_pluginProviderLoader_is_null() {
        pluginProviderLoader = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_if_pluginProviders_is_null() {
        given(pluginProviderLoader.getPluginProviders()).willReturn(null);

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_if_pluginProviders_is_empty() {
        given(pluginProviderLoader.getPluginProviders()).willReturn(Collections.emptyList());

        assertThrows(RuntimeException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_should_throw_if_pluginCreator_is_null() {
        pluginCreator = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    @SuppressWarnings("unchecked")
    void getPluginClass_should_return_first_plugin_found() {
        @SuppressWarnings("rawtypes")
        final Class expectedPluginClass = TestSink.class;

        given(firstPluginProvider.findPluginClass(baseClass, pluginName))
                .willReturn(expectedPluginClass);

        assertThat(createObjectUnderTest().getPluginClass(baseClass, pluginName),
                equalTo(expectedPluginClass));
    }
}