/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import com.amazon.dataprepper.parser.config.DataPrepperAppConfiguration;
import org.opensearch.dataprepper.logstash.LogstashConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Execute entry into Data Prepper.
 */
public class DataPrepperExecute {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperExecute.class);

    public static void main(String[] args) {
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        for (String arg : args) {
            LOG.info("Arg: {}", arg);
        }

        //TODO: Load this into context
        SimpleCommandLinePropertySource commandLinePropertySource = new SimpleCommandLinePropertySource(args);

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(commandLinePropertySource);
        context.register(DataPrepperAppConfiguration.class);
        context.refresh();

        DataPrepper dataPrepper = context.getBean(DataPrepper.class);
        dataPrepper.execute();
    }

    private static String checkForLogstashConfigurationAndConvert(String configurationFileLocation) {
        if (configurationFileLocation.endsWith(".conf")) {
            final LogstashConfigConverter logstashConfigConverter = new LogstashConfigConverter();
            final Path configurationDirectory = Paths.get(configurationFileLocation).toAbsolutePath().getParent();

            try {
                configurationFileLocation = logstashConfigConverter.convertLogstashConfigurationToPipeline(
                        configurationFileLocation, String.valueOf(configurationDirectory));
            } catch (IOException e) {
                LOG.error("Unable to read the Logstash configuration file", e);
            }
        }
        return configurationFileLocation;
    }
}