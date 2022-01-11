/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;

import java.io.File;
import java.util.List;

public class AggregateProcessorConfig {

    static int DEFAULT_WINDOW_DURATION = 180;
    static String DEFAULT_DB_PATH = "data/aggregate";

    @JsonProperty("identification_keys")
    @NotEmpty
    private List<String> identificationKeys;

    @JsonProperty("window_duration")
    @Min(0)
    private int windowDuration = DEFAULT_WINDOW_DURATION;

    @JsonProperty("db_path")
    @NotEmpty
    private String dbPath = DEFAULT_DB_PATH;

    @JsonIgnore
    private File dbFile;

    public List<String> getIdentificationKeys() {
        return identificationKeys;
    }

    public int getWindowDuration() {
        return windowDuration;
    }

    public String getDbPath() {
        return dbPath;
    }

    @AssertTrue(message = "db_path is not a valid file path")
    boolean isDbPathValid() {
        dbFile = new File(dbPath);
        return dbFile.exists() || dbFile.mkdirs();
    }

}
