/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.support.GenericApplicationContext;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Named
public class DataPrepperExtensionPoints implements ExtensionPoints {
    private static final ExtensionProvider.Context EMPTY_CONTEXT = new EmptyContext();
    private final GenericApplicationContext sharedApplicationContext;
    private final GenericApplicationContext coreApplicationContext;
    private Set<String> registeredBeanNames;

    @Inject
    public DataPrepperExtensionPoints(
            final PluginBeanFactoryProvider pluginBeanFactoryProvider) {
        Objects.requireNonNull(pluginBeanFactoryProvider);
        Objects.requireNonNull(pluginBeanFactoryProvider.getCoreApplicationContext());
        Objects.requireNonNull(pluginBeanFactoryProvider.getSharedPluginApplicationContext());
        this.sharedApplicationContext = pluginBeanFactoryProvider.getSharedPluginApplicationContext();
        this.coreApplicationContext = pluginBeanFactoryProvider.getCoreApplicationContext();
        this.registeredBeanNames = new HashSet<>();
    }

    @Override
    public void addExtensionProvider(final ExtensionProvider extensionProvider) {
        final String beanName = resolveBeanName(extensionProvider);
        if (registeredBeanNames.contains(beanName)) {
            return;
        }
        coreApplicationContext.registerBean(
                beanName,
                extensionProvider.supportedClass(),
                () -> extensionProvider.provideInstance(EMPTY_CONTEXT).orElse(null),
                b -> b.setScope(BeanDefinition.SCOPE_PROTOTYPE));
        sharedApplicationContext.registerBean(
                beanName,
                extensionProvider.supportedClass(),
                () -> extensionProvider.provideInstance(EMPTY_CONTEXT),
                b -> b.setScope(BeanDefinition.SCOPE_PROTOTYPE));
        registeredBeanNames.add(beanName);
    }

    @Override
    public <T> T getExtensionProvider(final Class<T> type) {
        sharedApplicationContext.refresh();
        return sharedApplicationContext.getBean(type);
    }

    private String resolveBeanName(final ExtensionProvider extensionProvider) {
        if (PluginConfigValueTranslator.class.isAssignableFrom(extensionProvider.supportedClass())) {
            return PluginConfigValueTranslator.class.getName() + "_" + extensionProvider.provideInstance(EMPTY_CONTEXT)
                    .filter(instance -> instance instanceof PluginConfigValueTranslator)
                    .map(instance -> ((PluginConfigValueTranslator) instance).getPrefix())
                    .orElse(extensionProvider.supportedClass().getName());
        }
        return extensionProvider.supportedClass().getName();
    }

    private static class EmptyContext implements ExtensionProvider.Context {
    }
}
