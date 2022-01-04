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

    private static String checkForLogstashConfigurationAndConvert(String configurationFileLocation) {
        if (configurationFileLocation.endsWith(".conf")) {
            LOG.debug("Detected logstash configuration file, attempting to convert to Data Prepper pipeline");

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

    private final String pipelineConfigFileLocation;
    private final String dataPrepperConfigFileLocation;

    public DataPrepperArgs(final String ... args) {
        if (args == null || args.length == 0) {
            LOG.error("Configuration file command line argument required but none found");
            System.exit(1);
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

    @Nullable
    public String getPipelineConfigFileLocation() {
        return pipelineConfigFileLocation;
    }

    public String getDataPrepperConfigFileLocation() {
        return dataPrepperConfigFileLocation;
    }
}
