/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;

/**
 * @since 1.3
 */
public class ContextManager extends AbstractContextManager {
    private static final Logger LOG = LoggerFactory.getLogger(ContextManager.class);
    private final SimpleCommandLinePropertySource commandLinePropertySource;

    /**
     * @since 1.3
     * @param args Application command line arguments
     * 
     * @see DataPrepperExecute#main(String...) 
     */
    public ContextManager(final String ... args) {
        LOG.trace("Reading args");
        commandLinePropertySource = new SimpleCommandLinePropertySource(args);
    }

    @Override
    protected void preRefreshCoreApplicationContext(final AnnotationConfigApplicationContext coreApplicationContext) {
        coreApplicationContext.getEnvironment().getPropertySources().addFirst(commandLinePropertySource);
    }
}
