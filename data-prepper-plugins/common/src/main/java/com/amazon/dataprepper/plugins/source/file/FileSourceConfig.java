package com.amazon.dataprepper.plugins.source.file;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FileSourceConfig {
    static final String ATTRIBUTE_PATH = "path";
    static final String ATTRIBUTE_TIMEOUT = "write_timeout";
    static final String ATTRIBUTE_TYPE = "type";
    static final String ATTRIBUTE_FORMAT = "format";
    static final int DEFAULT_TIMEOUT = 5_000;
    static final String DEFAULT_TYPE = "event";



    @JsonProperty(ATTRIBUTE_PATH)
    private String filePathToRead;

    @JsonProperty(ATTRIBUTE_TIMEOUT)
    private int writeTimeout = DEFAULT_TIMEOUT;

    @JsonProperty(ATTRIBUTE_FORMAT)
    private FileFormat format = FileFormat.PLAIN;

    @JsonProperty(ATTRIBUTE_TYPE)
    private String type = DEFAULT_TYPE;

    public String getFilePathToRead() {
        return filePathToRead;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public FileFormat getFormat() {
        return format;
    }

    public String getType() {
        return type;
    }
}
