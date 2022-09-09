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
 * <p>Creates Spring {@link org.springframework.context.ApplicationContext} hierarchy for Dependency Injection with limited visibility.</p>
 * <p>
 *     Application Context Hierarchy
 *         Public Application Context<br>
 *         ├─ Core Application Context<br>
 *         ├─ Shared Plugin Application Context<br>
 *             ├─ Plugin Isolated Application Context<br>
 * </p>
 */
public class ContextManager {
    private static final Logger LOG = LoggerFactory.getLogger(ContextManager.class);

    private final AnnotationConfigApplicationContext coreApplicationContext;

    private static final String DATA_PREPPER_HOME = System.getProperty("user.dir");
    private static final String PIPELINES_CONFIG = DATA_PREPPER_HOME + "/pipelines/pipelines.yaml";
    private static final String DATA_PREPPER_CONFIG = DATA_PREPPER_HOME + "/config/data-prepper-config.yaml";

    public ContextManager() {
        this(PIPELINES_CONFIG, DATA_PREPPER_CONFIG);
    }

    /**
     * @since 1.3
     * @param args Application command line arguments
     * 
     * @see DataPrepperExecute#main(String...) 
     */
    public ContextManager(final String ... args) {
        LOG.trace("Reading args");
        final SimpleCommandLinePropertySource commandLinePropertySource = new SimpleCommandLinePropertySource(args);

        final AnnotationConfigApplicationContext publicApplicationContext = new AnnotationConfigApplicationContext();
        publicApplicationContext.scan("org.opensearch.dataprepper.expression");
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
