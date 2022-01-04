/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.env.SimpleCommandLinePropertySource;

/**
 * Execute entry into Data Prepper.
 */
@ComponentScan
public class DataPrepperExecute {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperExecute.class);

    public static void main(String[] args) {
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        LOG.trace("Reading args");
        SimpleCommandLinePropertySource commandLinePropertySource = new SimpleCommandLinePropertySource(args);

        LOG.trace("Creating application context");
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(commandLinePropertySource);
        context.register(DataPrepperExecute.class);
        context.refresh();

        DataPrepper dataPrepper = context.getBean(DataPrepper.class);

        LOG.trace("Starting Data Prepper execution");
        dataPrepper.execute();
    }
}