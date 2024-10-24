/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core;

import org.opensearch.dataprepper.core.parser.config.FileStructurePathProvider;
import org.opensearch.dataprepper.logstash.LogstashConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

class DataPrepperArgs implements FileStructurePathProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperArgs.class);
    private static final Integer DATA_PREPPER_PIPELINE_CONFIG_POSITON = 0;
    private static final Integer DATA_PREPPER_CONFIG_POSITON = 1;
    private static final Integer MAXIMUM_SUPPORTED_NUMBER_OF_ARGS = 2;

    /**
     * Converts Logstash configuration to Data Prepper pipeline configuration. If input is a directory path, converts all Logstash
     * configurations in the directory and return the directory path; if input is a file path, convert the file if necessary and
     * return the pipeline configuration path.
     *
     * @param configurationFileLocation String path to a configuration or to a directory containing configurations
     * @return String path to a pipeline configuration or a directory containing pipeline configurations
     */
    private static String checkForLogstashConfigurationAndConvert(String configurationFileLocation) {
        final File configurationLocationAsFile = new File(configurationFileLocation);
        final LogstashConfigConverter logstashConfigConverter = new LogstashConfigConverter();
        final Path configurationDirectory;

        if (configurationLocationAsFile.isDirectory()) {
            configurationDirectory = Paths.get(configurationFileLocation).toAbsolutePath();
            final FileFilter confFilter = pathname -> (pathname.getName().endsWith(".conf"));
            for (final File file : configurationLocationAsFile.listFiles(confFilter)) {
                LOG.info("Detected logstash configuration file {}, attempting to convert to Data Prepper pipeline", file.getName());

                try {
                    logstashConfigConverter.convertLogstashConfigurationToPipeline(
                            file.getAbsolutePath(), String.valueOf(configurationDirectory));
                } catch (final IOException e) {
                    LOG.warn("Unable to read the Logstash configuration file", e);
                    throw new IllegalArgumentException("Invalid Logstash configuration file", e);
                }
            }
        } else if (configurationFileLocation.endsWith(".conf")) {
            LOG.debug("Detected logstash configuration file, attempting to convert to Data Prepper pipeline");

            configurationDirectory = Paths.get(configurationFileLocation).toAbsolutePath().getParent();

            try {
                configurationFileLocation = logstashConfigConverter.convertLogstashConfigurationToPipeline(
                        configurationFileLocation, String.valueOf(configurationDirectory));
            } catch (final IOException e) {
                LOG.warn("Unable to read the Logstash configuration file", e);
                throw new IllegalArgumentException("Invalid Logstash configuration file", e);
            }
        }
        return configurationFileLocation;
    }

    private final String pipelineConfigFileLocation;
    private final String dataPrepperConfigFileLocation;

    public DataPrepperArgs(final String ... args) {
        if (args == null || args.length == 0) {
            invalidArgumentsReceived("Configuration file command line argument required but none found");
        }
        else if (args.length > MAXIMUM_SUPPORTED_NUMBER_OF_ARGS) {
            invalidArgumentsReceived(
                    "Data Prepper supports a maximum of " + MAXIMUM_SUPPORTED_NUMBER_OF_ARGS + " command line arguments");
        }

        final String configurationFileLocation = args[DATA_PREPPER_PIPELINE_CONFIG_POSITON];
        LOG.info("Using {} configuration file", configurationFileLocation);

        this.pipelineConfigFileLocation = DataPrepperArgs.checkForLogstashConfigurationAndConvert(configurationFileLocation);

        if (args.length > DATA_PREPPER_CONFIG_POSITON) {
            this.dataPrepperConfigFileLocation = args[DATA_PREPPER_CONFIG_POSITON];
        }
        else {
            this.dataPrepperConfigFileLocation = null;
        }
    }

    private void invalidArgumentsReceived(final String msg) {
        LOG.warn("Invalid Data Prepper arguments received." +
                " Valid argument format: <pipeline-config-file-path> [<data-prepper-config-file-path>]");
        throw new IllegalArgumentException(msg);
    }

    @Override
    public String getPipelineConfigFileLocation() {
        return pipelineConfigFileLocation;
    }

    @Override
    @Nullable
    public String getDataPrepperConfigFileLocation() {
        return dataPrepperConfigFileLocation;
    }
}
