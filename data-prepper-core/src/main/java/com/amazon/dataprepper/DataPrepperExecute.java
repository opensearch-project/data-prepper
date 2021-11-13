/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper;

import org.opensearch.dataprepper.logstash.LogstashConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Execute entry into Data Prepper.
 */
public class DataPrepperExecute {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperExecute.class);

    public static void main(String[] args) {
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        if(args.length > 1) {
            DataPrepper.configure(args[1]);
        } else {
            DataPrepper.configureWithDefaults();
        }
        final DataPrepper dataPrepper = DataPrepper.getInstance();
        if (args.length > 0) {
            String configurationFileLocation = args[0];
            if (args[0].endsWith(".conf")) {
                LogstashConfigConverter logstashConfigConverter = new LogstashConfigConverter();
                try {
                    configurationFileLocation = logstashConfigConverter.convertLogstashConfigurationToPipeline(
                            args[0], String.valueOf(Paths.get("data-prepper-core/build/libs/")));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            dataPrepper.execute(configurationFileLocation);
        } else {
            LOG.error("Configuration file is required");
            System.exit(1);
        }
    }
}