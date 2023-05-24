/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperExtensionPointsTest {
    @Mock(lenient = true)
    private PluginBeanFactoryProvider pluginBeanFactoryProvider;

    @Mock(lenient = true)
    private GenericApplicationContext sharedApplicationContext;

    @Mock(lenient = true)
    private ExtensionProvider extensionProvider;

    private Class extensionClass;

    @BeforeEach
    void setUp() {
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

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void addExtensionProvider_should_registerBean() {
        createObjectUnderTest().addExtensionProvider(extensionProvider);

        verify(sharedApplicationContext).registerBean(eq(extensionClass), any(Supplier.class), any(BeanDefinitionCustomizer.class));
    }

    @Test
    void addExtensionProvider_should_registerBean_which_calls_provideInstance() {
        createObjectUnderTest().addExtensionProvider(extensionProvider);

        final ArgumentCaptor<Supplier<Object>> supplierArgumentCaptor =
                ArgumentCaptor.forClass(Supplier.class);

        verify(sharedApplicationContext).registerBean(eq(extensionClass), supplierArgumentCaptor.capture(), any(BeanDefinitionCustomizer.class));

        final Supplier<Object> extensionProviderSupplier = supplierArgumentCaptor.getValue();

        verify(extensionProvider, never()).provideInstance(any());

        Object providedInstance = mock(Object.class);
        when(extensionProvider.provideInstance(any())).thenReturn(Optional.of(providedInstance));
        final Object actualValueFromSupplier = extensionProviderSupplier.get();

        assertThat(actualValueFromSupplier, instanceOf(Optional.class));
        Optional<Object> optionalWrapper = (Optional) actualValueFromSupplier;
        assertThat(optionalWrapper.isPresent(), equalTo(true));
        final Object actualInstance = optionalWrapper.get();
        assertThat(actualInstance, equalTo(providedInstance));
        verify(extensionProvider).provideInstance(any());
    }

    @Test
    void addExtensionProvider_should_registerBean_as_prototype() {
        createObjectUnderTest().addExtensionProvider(extensionProvider);

        final ArgumentCaptor<BeanDefinitionCustomizer> beanDefinitionCustomizerArgumentCaptor =
                ArgumentCaptor.forClass(BeanDefinitionCustomizer.class);

        verify(sharedApplicationContext).registerBean(eq(extensionClass), any(Supplier.class), beanDefinitionCustomizerArgumentCaptor.capture());

        final BeanDefinitionCustomizer beanDefinitionCustomizer = beanDefinitionCustomizerArgumentCaptor.getValue();

        final BeanDefinition beanDefinition = mock(BeanDefinition.class);
        beanDefinitionCustomizer.customize(beanDefinition);

        verify(beanDefinition).setScope(BeanDefinition.SCOPE_PROTOTYPE);
    }
}
