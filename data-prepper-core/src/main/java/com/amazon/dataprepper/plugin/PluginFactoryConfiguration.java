/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.GenericApplicationContext;

import javax.inject.Named;

@Named("PluginFactoryConfiguration")
public class PluginFactoryConfiguration {
    public static final String PLUGIN_APPLICATION_CONTEXT_NAME = "PluginApplicationContext";
    private static final Logger LOG = LoggerFactory.getLogger(PluginFactoryConfiguration.class);

    @Bean(name = PLUGIN_APPLICATION_CONTEXT_NAME)
    public ApplicationContext getPluginApplicationContext(final ApplicationContext coreApplicationContext) {
        final ApplicationContext publicApplicationContext = coreApplicationContext.getParent();
        final GenericApplicationContext context = new GenericApplicationContext(publicApplicationContext);
        context.refresh();
        return context;
    }
}
