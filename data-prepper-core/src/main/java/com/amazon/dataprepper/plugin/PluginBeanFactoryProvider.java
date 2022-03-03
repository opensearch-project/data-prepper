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
    private final ApplicationContext sharedPluginApplicationContext;

    @Inject
    PluginBeanFactoryProvider(final ApplicationContext coreContext) {
        final ApplicationContext publicContext = Objects.requireNonNull(coreContext.getParent());
        sharedPluginApplicationContext = new GenericApplicationContext(publicContext) {
            @Override
            public String toString() {
                return "Plugin Shared Context!";
            }
        };
    }

    public BeanFactory get() {
        final GenericApplicationContext pluginIsolatedApplicationContext = new GenericApplicationContext(sharedPluginApplicationContext) {
            @Override
            public String toString() {
                return "Plugin Isolated Context!";
            }
        };
        return pluginIsolatedApplicationContext.getBeanFactory();
    }

    @Override
    public String toString() {
        return sharedPluginApplicationContext.toString();
    }
}
