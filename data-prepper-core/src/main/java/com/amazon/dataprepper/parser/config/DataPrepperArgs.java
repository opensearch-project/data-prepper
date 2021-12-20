package com.amazon.dataprepper.parser.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class DataPrepperArgs {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperArgs.class);

    private static final Integer DATA_PREPPER_PIPELINE_CONFIG_POSITON = 0;
    private static final Integer DATA_PREPPER_CONFIG_POSITON = 1;

    private final String pipelineConfigFileLocation;
    private final String dataPrepperConfigFileLocation;

    public DataPrepperArgs(final String ... args) {
        if (args == null || args.length == 0) {
            LOG.error("Configuration file command line argument required but none found");
            System.exit(1);
        }

        this.pipelineConfigFileLocation = args[DATA_PREPPER_PIPELINE_CONFIG_POSITON];
        LOG.info("Using {} configuration file", pipelineConfigFileLocation);

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
