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
    private static final Logger LOG = LoggerFactory.getLogger(PluginFactoryConfiguration.class);

    @Bean("PluginApplicationContext")
    public ApplicationContext pluginApplicationContext(final ApplicationContext coreApplicationContext) {
        if (coreApplicationContext == null || !coreApplicationContext.toString().equals("Core Context")) {
            LOG.error("Unexpected context wired ({})", coreApplicationContext);
        }

        final ApplicationContext publicApplicationContext = coreApplicationContext.getParent();

        final GenericApplicationContext context = new GenericApplicationContext() {
            @Override
            public String toString() {
                return "Plugin Context!";
            }
        };

        if (publicApplicationContext == null || !publicApplicationContext.toString().equals("Public Context")) {
            LOG.error("Incorrect context wired ({})", publicApplicationContext);
            context.setParent(coreApplicationContext);
        }
        else {
            context.setParent(publicApplicationContext);
        }
//        context.refresh();
        return context;
    }
}
