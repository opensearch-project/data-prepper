/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import com.amazon.dataprepper.plugin.PluginFactoryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;

class ContextManager {
    private static final Logger LOG = LoggerFactory.getLogger(ContextManager.class);

    private final AnnotationConfigApplicationContext publicContext;
    private final AnnotationConfigApplicationContext coreContext;

    public ContextManager(final String ... args) {
        LOG.trace("Reading args");
        final SimpleCommandLinePropertySource commandLinePropertySource = new SimpleCommandLinePropertySource(args);

        publicContext = new AnnotationConfigApplicationContext() {
            @Override
            public String toString() {
                return "Public Context";
            }
        };
        publicContext.register(PluginFactoryConfiguration.class);
        publicContext.refresh();

        coreContext = new AnnotationConfigApplicationContext() {
            @Override
            public String toString() {
                return "Core Context";
            }
        };
        coreContext.setParent(publicContext);
        coreContext.getEnvironment().getPropertySources().addFirst(commandLinePropertySource);
        coreContext.register(DataPrepperExecute.class);

        coreContext.refresh();
    }

    public DataPrepper getDataPrepperBean() {
        return coreContext.getBean(DataPrepper.class);
    }
}
