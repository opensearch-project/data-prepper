/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PluginBeanFactoryProviderTest {

    @Test
    void testPluginBeanFactoryProviderUsesParentContext() {
        final ApplicationContext context = mock(ApplicationContext.class);
        doReturn(context).when(context).getParent();

        final PluginBeanFactoryProvider beanFactoryProvider = new PluginBeanFactoryProvider(context);

        verify(context).getParent();
    }

    @Test
    void testPluginBeanFactoryProviderRequiresContext() {
        assertThrows(NullPointerException.class, () -> new PluginBeanFactoryProvider(null));
    }

    @Test
    void testPluginBeanFactoryProviderRequiresParentContext() {
        final ApplicationContext context = mock(ApplicationContext.class);

        assertThrows(NullPointerException.class, () -> new PluginBeanFactoryProvider(context));
    }

    @Test
    void testPluginBeanFactoryProviderGetReturnsBeanFactory() {
        final ApplicationContext context = mock(ApplicationContext.class);
        doReturn(context).when(context).getParent();

        final PluginBeanFactoryProvider beanFactoryProvider = new PluginBeanFactoryProvider(context);

        verify(context).getParent();
        assertThat(beanFactoryProvider.get(), is(instanceOf(BeanFactory.class)));
    }

    @Test
    void testPluginBeanFactoryProviderGetReturnsUniqueBeanFactory() {
        final ApplicationContext context = mock(ApplicationContext.class);
        doReturn(context).when(context).getParent();

        final PluginBeanFactoryProvider beanFactoryProvider = new PluginBeanFactoryProvider(context);

        verify(context).getParent();
        assertThat(beanFactoryProvider.get(), not(is(beanFactoryProvider.get())));
    }

}