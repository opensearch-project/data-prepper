/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.opensearch.dataprepper.core.DataPrepper;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;

import java.nio.file.Paths;

/**
 * Execute entry into Data Prepper.
 */
@ComponentScan
public class DataPrepperExecute {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperExecute.class);

    public static void main(final String ... args) {
        LOG.info("Data Prepper {}", DataPrepperVersion.getCurrentVersion());
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.apache.ApacheSdkHttpService");

        final ContextManager contextManager;
        if (args.length == 0) {
            final String dataPrepperHome = System.getProperty("data-prepper.dir");
            if (dataPrepperHome == null) {
                throw new RuntimeException("Data Prepper home directory (data-prepper.dir) not set in system properties.");
            }

            final String dataPrepperPipelines = Paths.get(dataPrepperHome).resolve("pipelines/").toString();
            final String dataPrepperConfig = Paths.get(dataPrepperHome).resolve("config/data-prepper-config.yaml").toString();
            contextManager = new ContextManager(dataPrepperPipelines, dataPrepperConfig);
        } else {
            contextManager = new ContextManager(args);
        }

        final DataPrepper dataPrepper = contextManager.getDataPrepperBean();

        // Register shutdown hook to jvm for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received. Initiating graceful shutdown of Data Prepper.");
            dataPrepper.shutdown();
            LOG.info("Data Prepper shutdown complete.");
        }));

        LOG.trace("Starting Data Prepper execution");
        dataPrepper.execute();
    }
}
