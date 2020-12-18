package com.amazon.dataprepper.parser.model;

import java.io.File;
import java.io.IOException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Class to hold configuration for DataPrepper, including server port and Log4j settings
 */
public class DataPrepperConfiguration {
    private int serverPort = 4900;
    private Log4JConfiguration log4JConfiguration = Log4JConfiguration.DEFAULT_CONFIG;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    public static final DataPrepperConfiguration DEFAULT_CONFIG = new DataPrepperConfiguration();

    /**
     * Construct a DataPrepperConfiguration from a yaml file
     * @param file
     * @return
     */
    public static DataPrepperConfiguration fromFile(File file) {
        try {
            return OBJECT_MAPPER.readValue(file, DataPrepperConfiguration.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid DataPrepper configuratino file.");
        }
    }

    private DataPrepperConfiguration() {}

    @JsonCreator
    public DataPrepperConfiguration(
            @JsonProperty("serverPort") final int serverPort,
            @JsonProperty("log4jConfig") final Log4JConfiguration log4JConfiguration
    ) {
        setServerPort(serverPort);
        setLog4JConfiguration(log4JConfiguration);
    }

    private void setServerPort(int serverPort) {
        if(serverPort != 0) {
            this.serverPort = serverPort;
        }
    }

    private void setLog4JConfiguration(Log4JConfiguration log4JConfiguration) {
        if(log4JConfiguration != null) {
            this.log4JConfiguration = log4JConfiguration;
        }
    }

    public int getServerPort() {
        return serverPort;
    }

    public Log4JConfiguration getLog4JConfiguration() {
        return log4JConfiguration;
    }
}
