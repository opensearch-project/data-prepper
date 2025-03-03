/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.test.TestComponent;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.support.GenericApplicationContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PluginBeanFactoryProviderTest {

    private GenericApplicationContext context;

    @BeforeEach
    void setUp() {
        context = mock(GenericApplicationContext.class);
    }

    private PluginBeanFactoryProvider createObjectUnderTest() {
        return new PluginBeanFactoryProvider(context);
    }

    @Test
    void testPluginBeanFactoryProviderUsesParentContext() {

        doReturn(context).when(context).getParent();

        createObjectUnderTest();

        verify(context).getParent();
    }

    @Test
    void testPluginBeanFactoryProviderRequiresContext() {
        context = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void testPluginBeanFactoryProviderRequiresParentContext() {
        context = mock(GenericApplicationContext.class);

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void testPluginBeanFactoryProviderGetReturnsBeanFactory() {
        doReturn(context).when(context).getParent();

        final PluginBeanFactoryProvider beanFactoryProvider = createObjectUnderTest();

        verify(context).getParent();
        assertThat(beanFactoryProvider.createPluginSpecificContext(new Class[]{}, null, null), is(instanceOf(BeanFactory.class)));
    }

    @Test
    void testPluginBeanFactoryProviderGetReturnsUniqueBeanFactory() {
        doReturn(context).when(context).getParent();

        final PluginBeanFactoryProvider beanFactoryProvider = createObjectUnderTest();
        final BeanFactory isolatedBeanFactoryA = beanFactoryProvider.createPluginSpecificContext(new Class[]{}, null, null);
        final BeanFactory isolatedBeanFactoryB = beanFactoryProvider.createPluginSpecificContext(new Class[]{}, null, null);

        verify(context).getParent();
        assertThat(isolatedBeanFactoryA, not(sameInstance(isolatedBeanFactoryB)));
    }

    @Test
    void getSharedPluginApplicationContext_returns_created_ApplicationContext() {
        doReturn(context).when(context).getParent();
        final GenericApplicationContext actualContext = createObjectUnderTest().getSharedPluginApplicationContext();

        assertThat(actualContext, notNullValue());
        assertThat(actualContext.getParent(), equalTo(context));
    }

    @Test
    void getSharedPluginApplicationContext_called_multiple_times_returns_same_instance() {
        doReturn(context).when(context).getParent();
        final PluginBeanFactoryProvider objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getSharedPluginApplicationContext(), sameInstance(objectUnderTest.getSharedPluginApplicationContext()));
    }

    @Test
    void testCreatePluginSpecificContext() {
        when(context.getParent()).thenReturn(context);
        final PluginBeanFactoryProvider objectUnderTest = createObjectUnderTest();
        BeanFactory beanFactory = objectUnderTest.createPluginSpecificContext(new Class[]{TestComponent.class}, null, null);
        assertThat(beanFactory, notNullValue());
        assertThat(beanFactory.getBean(TestComponent.class), notNullValue());
    }

    @Test
    void testCreatePluginSpecificContext_with_empty_array() {
        when(context.getParent()).thenReturn(context);
        final PluginBeanFactoryProvider objectUnderTest = createObjectUnderTest();
        BeanFactory beanFactory = objectUnderTest.createPluginSpecificContext(new Class[]{}, null, null);
        assertThat(beanFactory, notNullValue());
        assertThat(beanFactory, instanceOf(ListableBeanFactory.class));
        ListableBeanFactory listableBeanFactory = (ListableBeanFactory) beanFactory;
        List<String> nonSpringBeans = Arrays.stream(listableBeanFactory.getBeanDefinitionNames())
                .filter(Predicate.not(name -> name.startsWith("org.springframework")))
                .collect(Collectors.toList());
        assertThat(nonSpringBeans, equalTo(Collections.emptyList()));
    }

    @Test
    void testCreatePluginSpecificContext_with_pipeline_settings() {
        when(context.getParent()).thenReturn(context);
        final PluginBeanFactoryProvider objectUnderTest = createObjectUnderTest();
        PluginSetting pipelineSettings = new PluginSetting(UUID.randomUUID().toString(), Map.of("key", "val"));
        BeanFactory beanFactory = objectUnderTest.createPluginSpecificContext(new Class[]{}, pipelineSettings, pipelineSettings);
        assertThat(beanFactory, notNullValue());
        assertThrows(NoSuchBeanDefinitionException.class, () -> beanFactory.getBean(PluginSetting.class));
    }

    @Test
    void testCreatePluginSpecificContext_with_empty_array_with_plugin_config() {
        when(context.getParent()).thenReturn(context);
        final PluginBeanFactoryProvider objectUnderTest = createObjectUnderTest();
        TestPluginConfiguration config = new TestPluginConfiguration();
        BeanFactory beanFactory = objectUnderTest.createPluginSpecificContext(new Class[]{}, config, null);
        assertThat(beanFactory, notNullValue());
        assertThrows(NoSuchBeanDefinitionException.class, () -> beanFactory.getBean(TestComponent.class));
        assertThrows(NoSuchBeanDefinitionException.class, () -> beanFactory.getBean(TestPluginConfiguration.class));
    }

    @Test
    void testCreatePluginSpecificContext_with_plugin_config() {
        when(context.getParent()).thenReturn(context);
        final PluginBeanFactoryProvider objectUnderTest = createObjectUnderTest();
        TestPluginConfiguration config = new TestPluginConfiguration();
        String requiredStringValue = UUID.randomUUID().toString();
        config.setRequiredString(requiredStringValue);
        BeanFactory beanFactory = objectUnderTest.createPluginSpecificContext(new Class[]{TestComponent.class}, config, null);
        assertThat(beanFactory, notNullValue());
        assertThat(beanFactory.getBean(TestComponent.class), notNullValue());
        assertThat(beanFactory.getBean(TestPluginConfiguration.class), notNullValue());
        assertThat(beanFactory.getBean(TestPluginConfiguration.class).getRequiredString(), equalTo(requiredStringValue));
    }
}