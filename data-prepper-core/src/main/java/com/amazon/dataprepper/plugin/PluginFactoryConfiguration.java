/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.GenericApplicationContext;

import javax.inject.Named;

@Named
public class PluginFactoryConfiguration {

    @Bean
    public ApplicationContext getPluginApplicationContext(final ApplicationContext coreApplicationContext) {
        final ApplicationContext publicApplicationContext = coreApplicationContext.getParent();
        final GenericApplicationContext context = new GenericApplicationContext(publicApplicationContext);
        context.refresh();
        return context;
    }
}
