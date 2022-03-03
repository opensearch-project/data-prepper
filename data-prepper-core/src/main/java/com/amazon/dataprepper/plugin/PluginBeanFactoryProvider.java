/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.Objects;

@Named
class PluginBeanFactoryProvider implements Provider<BeanFactory> {
    private static final Logger LOG = LoggerFactory.getLogger(PluginBeanFactoryProvider.class);
    private final ApplicationContext pluginApplicationContext;

    @Inject
    PluginBeanFactoryProvider(final ApplicationContext coreContext) {
        final ApplicationContext publicContext = Objects.requireNonNull(coreContext.getParent());
        pluginApplicationContext = new GenericApplicationContext(publicContext) {
            @Override
            public String toString() {
                return "Plugin Context!";
            }
        };

        LOG.error("PluginBeanFactoryProvider made with context: {}", coreContext);
    }

    public BeanFactory get() {
        final GenericApplicationContext pluginInstanceContext = new GenericApplicationContext(pluginApplicationContext);
        return pluginInstanceContext.getBeanFactory();
    }

    @Override
    public String toString() {
        return pluginApplicationContext.toString();
    }
}
