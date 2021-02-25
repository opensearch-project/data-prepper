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
    private boolean ssl = true;
    private String sslKeyFile = "";
    private String sslKeyCertChainFile = "";
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
            throw new IllegalArgumentException("Invalid DataPrepper configuration file.");
        }
    }

    private DataPrepperConfiguration() {}

    @JsonCreator
    public DataPrepperConfiguration(
            @JsonProperty("ssl") final Boolean ssl,
            @JsonProperty("sslKeyFile") final String sslKeyFile,
            @JsonProperty("sslKeyCertChainFile") final String sslKeyCertChainFile,
            @JsonProperty("serverPort") final String serverPort,
            @JsonProperty("log4jConfig") final Log4JConfiguration log4JConfiguration
    ) {
        setSsl(ssl);
        this.sslKeyFile = sslKeyFile;
        this.sslKeyCertChainFile = sslKeyCertChainFile;
        setServerPort(serverPort);
        setLog4JConfiguration(log4JConfiguration);
    }

    public int getServerPort() {
        return serverPort;
    }

    public Log4JConfiguration getLog4JConfiguration() {
        return log4JConfiguration;
    }

    public boolean ssl() {
        return ssl;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public String getSslKeyCertChainFile() {
        return sslKeyCertChainFile;
    }

    private void setSsl(final Boolean ssl) {
        if (ssl != null) {
            this.ssl = ssl;
        }
    }

    private void setServerPort(final String serverPort) {
        if(serverPort != null && !serverPort.isEmpty()) {
            try {
                int port = Integer.parseInt(serverPort);
                if(port <= 0) {
                    throw new IllegalArgumentException("Server port must be a positive integer");
                }
                this.serverPort = port;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Server port must be a positive integer");
            }
        }
    }

    private void setLog4JConfiguration(Log4JConfiguration log4JConfiguration) {
        if(log4JConfiguration != null) {
            this.log4JConfiguration = log4JConfiguration;
        }
    }
}
