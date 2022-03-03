/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;

/**
 * @since 1.3
 * <p>Creates Spring {@link org.springframework.context.ApplicationContext} hierarchy for Dependency Injection with limited visibility.</p>
 * <p>
 *     Application Context Hierarchy
 *     <pre>
 *         Public Application Context<br>
 *         ├─ Core Application Context<br>
 *         ├─ Shared Plugin Application Context<br>
 *             ├─ Plugin Isolated Application Context<br>
 *     </pre>
 * </p>
 */
class ContextManager {
    private static final Logger LOG = LoggerFactory.getLogger(ContextManager.class);

    private final GenericApplicationContext publicApplicationContext;
    private final AnnotationConfigApplicationContext coreApplicationContext;

    public ContextManager(final String ... args) {
        LOG.trace("Reading args");
        final SimpleCommandLinePropertySource commandLinePropertySource = new SimpleCommandLinePropertySource(args);

        publicApplicationContext = new GenericApplicationContext();
        publicApplicationContext.refresh();

        coreApplicationContext = new AnnotationConfigApplicationContext();
        coreApplicationContext.setParent(publicApplicationContext);
        coreApplicationContext.getEnvironment().getPropertySources().addFirst(commandLinePropertySource);
        coreApplicationContext.register(DataPrepperExecute.class);

        coreApplicationContext.refresh();
    }

    /**
     * @since 1.3
     * Retrieves the instance of {@link DataPrepper} singleton
     * @return {@link DataPrepper} instance
     */
    public DataPrepper getDataPrepperBean() {
        return coreApplicationContext.getBean(DataPrepper.class);
    }
}
