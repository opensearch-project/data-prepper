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
    private String keyStoreFilePath = "";
    private String keyStorePassword = "";
    private String privateKeyPassword = "";

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
            @JsonProperty("keyStoreFilePath") final String keyStoreFilePath,
            @JsonProperty("keyStorePassword") final String keyStorePassword,
            @JsonProperty("privateKeyPassword") final String privateKeyPassword,
            @JsonProperty("serverPort") final String serverPort
    ) {
        setSsl(ssl);
        this.keyStoreFilePath = keyStoreFilePath != null ? keyStoreFilePath : "";
        this.keyStorePassword = keyStorePassword != null ? keyStorePassword : "";
        this.privateKeyPassword = privateKeyPassword != null ? privateKeyPassword : "";
        setServerPort(serverPort);
    }

    public int getServerPort() {
        return serverPort;
    }

    public boolean ssl() {
        return ssl;
    }

    public String getKeyStoreFilePath() {
        return keyStoreFilePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getPrivateKeyPassword() {
        return privateKeyPassword;
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
}
