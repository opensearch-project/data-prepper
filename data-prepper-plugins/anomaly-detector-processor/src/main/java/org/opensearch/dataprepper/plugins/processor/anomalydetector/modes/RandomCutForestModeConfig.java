/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector.modes;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;
import java.util.HashSet;

public class RandomCutForestModeConfig {
    public static final int DEFAULT_SHINGLE_SIZE = 4;
    private static final int MIN_SHINGLE_SIZE = 1;
    public static final int MAX_SHINGLE_SIZE = 60;
    public static final int DEFAULT_SAMPLE_SIZE = 256;
    private static final int MIN_SAMPLE_SIZE = 100;
    public static final int MAX_SAMPLE_SIZE = 2500;
    public static final double DEFAULT_TIME_DECAY = 0.1;
    private static final double MIN_TIME_DECAY = 0.0;
    public static final double MAX_TIME_DECAY = 1.0;

    public static final String VERSION_1_0 = "1.0";

    @JsonProperty("version")
    private String version = VERSION_1_0;
    
    public static final Set<String> validVersions = new HashSet<>(Set.of(VERSION_1_0));

    @JsonProperty("type")
    private String type = RandomCutForestType.METRICS.toString();

    public static final Set<String> validTypes = new HashSet<>(Set.of(RandomCutForestType.METRICS.toString()));

    @JsonProperty("shingleSize")
    private int shingleSize = DEFAULT_SHINGLE_SIZE;

    @JsonProperty("sampleSize")
    private int sampleSize = DEFAULT_SAMPLE_SIZE;

    @JsonProperty("timeDecay")
    private double timeDecay = DEFAULT_TIME_DECAY;

    public int getShingleSize() {
        if (shingleSize < MIN_SHINGLE_SIZE || shingleSize > MAX_SHINGLE_SIZE) {
            throw new IllegalArgumentException(String.format("Shingle size of %d is not valid, valid range is %d - %d", shingleSize, MIN_SHINGLE_SIZE, MAX_SHINGLE_SIZE));
        }
        return shingleSize;
    }

    public int getSampleSize() {
        if (sampleSize < MIN_SAMPLE_SIZE || sampleSize > MAX_SAMPLE_SIZE) {
            throw new IllegalArgumentException(String.format("Sample size of %d is not valid, valid range is %d - %d", sampleSize, MIN_SAMPLE_SIZE, MAX_SAMPLE_SIZE));
        }
        return sampleSize;
    }

    public double getTimeDecay() {
        if (timeDecay < MIN_TIME_DECAY || timeDecay > MAX_TIME_DECAY) {
            throw new IllegalArgumentException(String.format("Time Decay of %f is not valid, valid range is %f - %f", timeDecay, MIN_TIME_DECAY, MAX_TIME_DECAY));
        }
        return timeDecay;
    }

    public String getType() {
        if (!validTypes.contains(type)) {
            throw new IllegalArgumentException("Unknown type " + type);
        }
        return type;
    }

    public String getVersion() {
        if (!validVersions.contains(version)) {
            throw new IllegalArgumentException("Unknown version " + version);
        }
        return version;
    }
}
