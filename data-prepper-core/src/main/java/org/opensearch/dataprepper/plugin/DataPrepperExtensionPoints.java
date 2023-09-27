/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.support.GenericApplicationContext;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Objects;

@Named
public class DataPrepperExtensionPoints implements ExtensionPoints {
    private static final ExtensionProvider.Context EMPTY_CONTEXT = new EmptyContext();
    private final GenericApplicationContext sharedApplicationContext;
    private final GenericApplicationContext coreApplicationContext;

    @Inject
    public DataPrepperExtensionPoints(
            final PluginBeanFactoryProvider pluginBeanFactoryProvider) {
        Objects.requireNonNull(pluginBeanFactoryProvider);
        Objects.requireNonNull(pluginBeanFactoryProvider.getCoreApplicationContext());
        Objects.requireNonNull(pluginBeanFactoryProvider.getSharedPluginApplicationContext());
        this.sharedApplicationContext = pluginBeanFactoryProvider.getSharedPluginApplicationContext();
        this.coreApplicationContext = pluginBeanFactoryProvider.getCoreApplicationContext();
    }

    @Override
    public void addExtensionProvider(final ExtensionProvider extensionProvider) {
        coreApplicationContext.registerBean(
                extensionProvider.supportedClass(),
                () -> extensionProvider.provideInstance(EMPTY_CONTEXT).orElse(null),
                b -> b.setScope(BeanDefinition.SCOPE_PROTOTYPE));
        sharedApplicationContext.registerBean(
                extensionProvider.supportedClass(),
                () -> extensionProvider.provideInstance(EMPTY_CONTEXT),
                b -> b.setScope(BeanDefinition.SCOPE_PROTOTYPE));
    }

    private static class EmptyContext implements ExtensionProvider.Context {

    }
}
