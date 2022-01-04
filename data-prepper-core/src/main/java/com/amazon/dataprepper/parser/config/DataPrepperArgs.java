package com.amazon.dataprepper.parser.config;

import org.opensearch.dataprepper.logstash.LogstashConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DataPrepperArgs {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperArgs.class);

    private static final Integer DATA_PREPPER_PIPELINE_CONFIG_POSITON = 0;
    private static final Integer DATA_PREPPER_CONFIG_POSITON = 1;
    private static final Integer MAXIMUM_SUPPORTED_NUMBER_OF_ARGS = 2;

    private static String checkForLogstashConfigurationAndConvert(String configurationFileLocation) {
        if (configurationFileLocation.endsWith(".conf")) {
            LOG.debug("Detected logstash configuration file, attempting to convert to Data Prepper pipeline");

            final LogstashConfigConverter logstashConfigConverter = new LogstashConfigConverter();
            final Path configurationDirectory = Paths.get(configurationFileLocation).toAbsolutePath().getParent();

            try {
                configurationFileLocation = logstashConfigConverter.convertLogstashConfigurationToPipeline(
                        configurationFileLocation, String.valueOf(configurationDirectory));
            } catch (IOException e) {
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

        String configurationFileLocation = args[DATA_PREPPER_PIPELINE_CONFIG_POSITON];
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

    @Nullable
    public String getPipelineConfigFileLocation() {
        return pipelineConfigFileLocation;
    }

    public String getDataPrepperConfigFileLocation() {
        return dataPrepperConfigFileLocation;
    }
}
