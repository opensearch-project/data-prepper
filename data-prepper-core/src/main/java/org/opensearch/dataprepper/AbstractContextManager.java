/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Base class for managing context. It provides hooks for inheritors to control the context to some degree.
 * <p>Creates Spring {@link org.springframework.context.ApplicationContext} hierarchy for Dependency Injection with limited visibility.</p>
 * <p>
 *     Application Context Hierarchy
 *         Public Application Context<br>
 *         ├─ Core Application Context<br>
 *         ├─ Shared Plugin Application Context<br>
 *             ├─ Plugin Isolated Application Context<br>
 * </p>
 * @since 2.2
 */
public abstract class AbstractContextManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractContextManager.class);
    private static final String BASE_DATA_PREPPER_PACKAGE = "org.opensearch.dataprepper";
    private static final String EXPRESSION_PACKAGE = BASE_DATA_PREPPER_PACKAGE + ".expression";

    private final AnnotationConfigApplicationContext publicApplicationContext;
    private final AnnotationConfigApplicationContext coreApplicationContext;
    private DataPrepper dataPrepper;

    protected AbstractContextManager() {
        publicApplicationContext = new AnnotationConfigApplicationContext();
        coreApplicationContext = new AnnotationConfigApplicationContext();
    }

    /**
     * @since 1.3
     * Retrieves the instance of {@link DataPrepper} singleton
     * @return {@link DataPrepper} instance
     */
    public DataPrepper getDataPrepperBean() {
        if(dataPrepper == null) {
            start();
        }
        return dataPrepper;
    }

    private void start() {
        publicApplicationContext.scan(EXPRESSION_PACKAGE);
        preRefreshPublicApplicationContext(publicApplicationContext);

        publicApplicationContext.refresh();
        coreApplicationContext.setParent(publicApplicationContext);
        coreApplicationContext.scan(BASE_DATA_PREPPER_PACKAGE);
        preRefreshCoreApplicationContext(coreApplicationContext);

        coreApplicationContext.refresh();

        dataPrepper = coreApplicationContext.getBean(DataPrepper.class);
        dataPrepper.registerShutdownHandler(new ContextShutdownListener());

        LOG.trace("Data Prepper context is fully refreshed.");
    }

    /**
     * Override this method to modify the public {@link AnnotationConfigApplicationContext} before
     * the {@link AbstractContextManager} calls {@link AnnotationConfigApplicationContext#refresh()}
     *
     * @param publicApplicationContext The public application context
     */
    protected void preRefreshPublicApplicationContext(final AnnotationConfigApplicationContext publicApplicationContext) {

    }

    /**
     * Override this method to modify the core {@link AnnotationConfigApplicationContext} before
     * the {@link AbstractContextManager} calls {@link AnnotationConfigApplicationContext#refresh()}
     *
     * @param coreApplicationContext The core application context
     */
    protected void preRefreshCoreApplicationContext(final AnnotationConfigApplicationContext coreApplicationContext) {

    }

    public void shutdown() {
        coreApplicationContext.close();
        publicApplicationContext.close();
    }

    private class ContextShutdownListener implements DataPrepperShutdownListener {
        @Override
        public void handleShutdown() {
            shutdown();
        }
    }
}
