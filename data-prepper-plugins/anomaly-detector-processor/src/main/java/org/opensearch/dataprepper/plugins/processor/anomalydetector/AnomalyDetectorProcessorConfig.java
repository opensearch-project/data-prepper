/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class AnomalyDetectorProcessorConfig {
    public static final String DEFAULT_MODE="random-cut-forest.metrics.v1";
    public static final int DEFAULT_SHINGLE_SIZE = 4;
    private static final int MIN_SHINGLE_SIZE = 1;
    public static final int MAX_SHINGLE_SIZE = 60;
    public static final int DEFAULT_SAMPLE_SIZE = 256;
    private static final int MIN_SAMPLE_SIZE = 100;
    public static final int MAX_SAMPLE_SIZE = 2500;
    public static final double DEFAULT_TIME_DECAY = 0.1;
    private static final double MIN_TIME_DECAY = 0.0;
    public static final double MAX_TIME_DECAY = 1.0;

    @JsonProperty("mode")
    private String mode = DEFAULT_MODE;

    @JsonProperty("shingleSize")
    private int shingleSize = DEFAULT_SHINGLE_SIZE;

    @JsonProperty("sampleSize")
    private int sampleSize = DEFAULT_SAMPLE_SIZE;

    @JsonProperty("timeDecay")
    private double timeDecay = DEFAULT_TIME_DECAY;

    @JsonProperty("keys")
    @NotEmpty
    private List<String> keys;

    public String getMode() {
        if (mode != AnomalyDetectorProcessorConfig.DEFAULT_MODE) {
            throw new IllegalArgumentException(String.format("Invalid mode %s", mode));
        }
        return mode;
    }

    public int getShingleSize() {
        if (shingleSize < MIN_SHINGLE_SIZE || shingleSize > MAX_SHINGLE_SIZE) {
            throw new IllegalArgumentException(String.format("Shingle size of %d is not valid", shingleSize));
        }
        return shingleSize;
    }

    public int getSampleSize() {
        if (sampleSize < MIN_SAMPLE_SIZE || sampleSize > MAX_SAMPLE_SIZE) {
            throw new IllegalArgumentException(String.format("Sample size of %d is not valid", sampleSize));
        }
        return sampleSize;
    }

    public double getTimeDecay() {
        if (timeDecay < MIN_TIME_DECAY || timeDecay > MAX_TIME_DECAY) {
            throw new IllegalArgumentException(String.format("Time Decay of %f is not valid", timeDecay));
        }
        return timeDecay;
    }

    public List<String> getKeys() {
        return keys;
    }

}
