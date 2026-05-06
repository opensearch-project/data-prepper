/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.test.TestExtension;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionCustomizer;
import org.springframework.context.support.GenericApplicationContext;

import java.util.Optional;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperExtensionPointsTest {
    @Mock(lenient = true)
    private PluginBeanFactoryProvider pluginBeanFactoryProvider;

    @Mock(lenient = true)
    private GenericApplicationContext sharedApplicationContext;

    @Mock(lenient = true)
    private GenericApplicationContext coreApplicationContext;

    @Mock(lenient = true)
    private ExtensionProvider extensionProvider;

    private Class extensionClass;

    @BeforeEach
    void setUp() {
        when(pluginBeanFactoryProvider.getCoreApplicationContext())
                .thenReturn(coreApplicationContext);
        when(pluginBeanFactoryProvider.getSharedPluginApplicationContext())
                .thenReturn(sharedApplicationContext);

        extensionClass = TestExtension.TestModel.class;
        when(extensionProvider.supportedClass()).thenReturn(extensionClass);
    }

    private DataPrepperExtensionPoints createObjectUnderTest() {
        return new DataPrepperExtensionPoints(pluginBeanFactoryProvider);
    }

    @Test
    void constructor_throws_if_provider_is_null() {
        pluginBeanFactoryProvider = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_provider_getSharedPluginApplicationContext_is_null() {
        reset(pluginBeanFactoryProvider);
        when(pluginBeanFactoryProvider.getCoreApplicationContext())
                .thenReturn(coreApplicationContext);
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_provider_getCoreApplicationContext_is_null() {
        reset(pluginBeanFactoryProvider);
        when(pluginBeanFactoryProvider.getSharedPluginApplicationContext())
                .thenReturn(sharedApplicationContext);
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void addExtensionProvider_should_registerBean() {
        createObjectUnderTest().addExtensionProvider(extensionProvider);

        verify(sharedApplicationContext).registerBean(anyString(), eq(extensionClass), any(Supplier.class), any(BeanDefinitionCustomizer.class));
        verify(coreApplicationContext).registerBean(anyString(), eq(extensionClass), any(Supplier.class), any(BeanDefinitionCustomizer.class));
    }

    @Test
    void addExtensionProvider_should_not_registerBean_for_same_provider_twice() {
        final DataPrepperExtensionPoints objectUnderTest = createObjectUnderTest();
        objectUnderTest.addExtensionProvider(extensionProvider);
        objectUnderTest.addExtensionProvider(extensionProvider);

        verify(sharedApplicationContext, times(1)).registerBean(anyString(), eq(extensionClass), any(Supplier.class), any(BeanDefinitionCustomizer.class));
        verify(coreApplicationContext, times(1)).registerBean(anyString(), eq(extensionClass), any(Supplier.class), any(BeanDefinitionCustomizer.class));
    }

    @Test
    void addExtensionProvider_should_registerBean_as_prototype() {
        createObjectUnderTest().addExtensionProvider(extensionProvider);

        verifyRegisteredAsPrototype(sharedApplicationContext);
        verifyRegisteredAsPrototype(coreApplicationContext);
    }

    @Test
    void addExtensionProvider_two_PluginConfigValueTranslators_with_different_prefixes_both_registered() {
        final ExtensionProvider<PluginConfigValueTranslator> firstProvider = translatorProvider("env");
        final ExtensionProvider<PluginConfigValueTranslator> secondProvider = translatorProvider("file");

        final DataPrepperExtensionPoints objectUnderTest = createObjectUnderTest();
        objectUnderTest.addExtensionProvider(firstProvider);
        objectUnderTest.addExtensionProvider(secondProvider);

        verify(sharedApplicationContext, times(2)).registerBean(anyString(), eq(PluginConfigValueTranslator.class), any(Supplier.class), any(BeanDefinitionCustomizer.class));
        verify(coreApplicationContext, times(2)).registerBean(anyString(), eq(PluginConfigValueTranslator.class), any(Supplier.class), any(BeanDefinitionCustomizer.class));
    }

    @Test
    void addExtensionProvider_two_PluginConfigValueTranslators_same_prefix_only_first_registered() {
        final ExtensionProvider<PluginConfigValueTranslator> firstProvider = translatorProvider("env");
        final ExtensionProvider<PluginConfigValueTranslator> duplicateProvider = translatorProvider("env");

        final DataPrepperExtensionPoints objectUnderTest = createObjectUnderTest();
        objectUnderTest.addExtensionProvider(firstProvider);
        objectUnderTest.addExtensionProvider(duplicateProvider);

        verify(sharedApplicationContext, times(1)).registerBean(anyString(), eq(PluginConfigValueTranslator.class), any(Supplier.class), any(BeanDefinitionCustomizer.class));
        verify(coreApplicationContext, times(1)).registerBean(anyString(), eq(PluginConfigValueTranslator.class), any(Supplier.class), any(BeanDefinitionCustomizer.class));
    }

    @Test
    void addExtensionProvider_three_PluginConfigValueTranslators_all_different_prefixes_all_registered() {
        final ExtensionProvider<PluginConfigValueTranslator> envProvider = translatorProvider("env");
        final ExtensionProvider<PluginConfigValueTranslator> fileProvider = translatorProvider("file");
        final ExtensionProvider<PluginConfigValueTranslator> awsProvider = translatorProvider("aws_secrets");

        final DataPrepperExtensionPoints objectUnderTest = createObjectUnderTest();
        objectUnderTest.addExtensionProvider(envProvider);
        objectUnderTest.addExtensionProvider(fileProvider);
        objectUnderTest.addExtensionProvider(awsProvider);

        verify(sharedApplicationContext, times(3)).registerBean(anyString(), eq(PluginConfigValueTranslator.class), any(Supplier.class), any(BeanDefinitionCustomizer.class));
        verify(coreApplicationContext, times(3)).registerBean(anyString(), eq(PluginConfigValueTranslator.class), any(Supplier.class), any(BeanDefinitionCustomizer.class));
    }

    @Test
    void addExtensionProvider_bean_name_for_translator_contains_prefix() {
        final ArgumentCaptor<String> beanNameCaptor = ArgumentCaptor.forClass(String.class);
        final ExtensionProvider<PluginConfigValueTranslator> provider = translatorProvider("env");

        createObjectUnderTest().addExtensionProvider(provider);

        verify(sharedApplicationContext).registerBean(beanNameCaptor.capture(), eq(PluginConfigValueTranslator.class), any(Supplier.class), any(BeanDefinitionCustomizer.class));
        assertThat(beanNameCaptor.getValue().contains("env"), equalTo(true));
    }

    @Test
    void addExtensionProvider_should_registerBean_which_calls_provideInstance() {
        createObjectUnderTest().addExtensionProvider(extensionProvider);

        verifyRegisterBeanWithProvideInstance(sharedApplicationContext);
        verifyRegisterBeanWithProvideInstanceOrElse(coreApplicationContext);
    }

    @Test
    void getExtensionProvider_refreshes_shared_context_and_returns_correct_bean() {
        final Class<DefaultPluginFactory> defaultPluginFactoryClass = DefaultPluginFactory.class;
        final DefaultPluginFactory defaultPluginFactory = mock(DefaultPluginFactory.class);
        when(sharedApplicationContext.getBean(defaultPluginFactoryClass)).thenReturn(defaultPluginFactory);

        final DefaultPluginFactory result = createObjectUnderTest().getExtensionProvider(defaultPluginFactoryClass);

        assertThat(result, equalTo(defaultPluginFactory));
        verify(sharedApplicationContext).refresh();
    }

    private ExtensionProvider<PluginConfigValueTranslator> translatorProvider(final String prefix) {
        final PluginConfigValueTranslator translator = new PluginConfigValueTranslator() {
            @Override
            public Object translate(final String value) {
                return value;
            }

            @Override
            public String getPrefix() {
                return prefix;
            }

            @Override
            public PluginConfigVariable translateToPluginConfigVariable(final String value) {
                return null;
            }
        };

        @SuppressWarnings("unchecked")
        final ExtensionProvider<PluginConfigValueTranslator> provider = mock(ExtensionProvider.class);
        when(provider.supportedClass()).thenReturn(PluginConfigValueTranslator.class);
        when(provider.provideInstance(any())).thenReturn(Optional.of(translator));
        return provider;
    }

    private void verifyRegisterBeanWithProvideInstance(final GenericApplicationContext applicationContext) {
        reset(extensionProvider);
        final ArgumentCaptor<Supplier<Object>> supplierCaptor = ArgumentCaptor.forClass(Supplier.class);

        verify(applicationContext).registerBean(anyString(), eq(extensionClass), supplierCaptor.capture(), any(BeanDefinitionCustomizer.class));

        verify(extensionProvider, never()).provideInstance(any());

        final Object providedInstance = mock(Object.class);
        when(extensionProvider.provideInstance(any())).thenReturn(Optional.of(providedInstance));
        final Object actual = supplierCaptor.getValue().get();

        assertThat(actual, instanceOf(Optional.class));
        assertThat(((Optional<Object>) actual).get(), equalTo(providedInstance));
    }

    private void verifyRegisterBeanWithProvideInstanceOrElse(final GenericApplicationContext applicationContext) {
        reset(extensionProvider);
        final ArgumentCaptor<Supplier<Object>> supplierCaptor = ArgumentCaptor.forClass(Supplier.class);

        verify(applicationContext).registerBean(anyString(), eq(extensionClass), supplierCaptor.capture(), any(BeanDefinitionCustomizer.class));

        verify(extensionProvider, never()).provideInstance(any());

        final Object providedInstance = mock(Object.class);
        when(extensionProvider.provideInstance(any())).thenReturn(Optional.of(providedInstance));
        final Object actual = supplierCaptor.getValue().get();

        assertThat(actual, equalTo(providedInstance));
    }

    private void verifyRegisteredAsPrototype(final GenericApplicationContext applicationContext) {
        final ArgumentCaptor<BeanDefinitionCustomizer> customizerCaptor =
                ArgumentCaptor.forClass(BeanDefinitionCustomizer.class);

        verify(applicationContext).registerBean(anyString(), eq(extensionClass), any(Supplier.class), customizerCaptor.capture());

        final BeanDefinition beanDefinition = mock(BeanDefinition.class);
        customizerCaptor.getValue().customize(beanDefinition);

        verify(beanDefinition).setScope(BeanDefinition.SCOPE_PROTOTYPE);
    }
}
