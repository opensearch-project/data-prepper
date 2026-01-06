/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector.modes;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_SHINGLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_SAMPLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_OUTPUT_AFTER;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_TIME_DECAY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.MAX_SHINGLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.MAX_SAMPLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.MAX_TIME_DECAY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.VERSION_1_0;

public class RandomCutForestModeConfigTests {
    @Test
    public void testDefault() {
        final RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        assertThat(randomCutForestModeConfig.getShingleSize(), equalTo(DEFAULT_SHINGLE_SIZE));
        assertThat(randomCutForestModeConfig.getSampleSize(), equalTo(DEFAULT_SAMPLE_SIZE));
        assertThat(randomCutForestModeConfig.getOutputAfter(), equalTo(DEFAULT_OUTPUT_AFTER));
        assertThat(randomCutForestModeConfig.getTimeDecay(), equalTo(DEFAULT_TIME_DECAY));
        assertThat(randomCutForestModeConfig.getType(), equalTo(RandomCutForestType.METRICS.toString()));
        assertThat(randomCutForestModeConfig.getVersion(), equalTo(VERSION_1_0));
    }

    @Test
    public void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        final int testShingleSize = 10;
        final int testSampleSize = 500;
        final int testOutputAfter = 300;
        final double testTimeDecay = 0.44;
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "shingleSize", testShingleSize);
        assertThat(randomCutForestModeConfig.getShingleSize(), equalTo(testShingleSize));
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "sampleSize", testSampleSize);
        assertThat(randomCutForestModeConfig.getSampleSize(), equalTo(testSampleSize));
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "outputAfter", testOutputAfter);
        assertThat(randomCutForestModeConfig.getOutputAfter(), equalTo(testOutputAfter));
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "timeDecay", testTimeDecay);
        assertThat(randomCutForestModeConfig.getTimeDecay(), equalTo(testTimeDecay));
        assertThat(randomCutForestModeConfig.getType(), equalTo(RandomCutForestType.METRICS.toString()));
        assertThat(randomCutForestModeConfig.getVersion(), equalTo(VERSION_1_0));
    }

    @Test
    public void testInvalidConfigShingleSize() throws NoSuchFieldException, IllegalAccessException {
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "shingleSize", MAX_SHINGLE_SIZE+1);
        assertThrows(IllegalArgumentException.class, () -> randomCutForestModeConfig.getShingleSize());
    }

    @Test
    public void testInvalidConfigSampleSize() throws NoSuchFieldException, IllegalAccessException {
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "sampleSize", MAX_SAMPLE_SIZE+1);
        assertThrows(IllegalArgumentException.class, () -> randomCutForestModeConfig.getSampleSize());
    }

    @Test
    public void testInvalidOutputAfter() throws NoSuchFieldException, IllegalAccessException {
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "outputAfter", MAX_SAMPLE_SIZE+1);
        assertThrows(IllegalArgumentException.class, () -> randomCutForestModeConfig.getOutputAfter());
    }

    @Test
    public void testInvalidConfigTimeDecay() throws NoSuchFieldException, IllegalAccessException {
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "timeDecay", MAX_TIME_DECAY+1);
        assertThrows(IllegalArgumentException.class, () -> randomCutForestModeConfig.getTimeDecay());
    }

    @Test
    public void testInvalidConfigVersion() throws NoSuchFieldException, IllegalAccessException {
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "version", UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> randomCutForestModeConfig.getVersion());
    }

    @Test
    public void testInvalidConfigType() throws NoSuchFieldException, IllegalAccessException {
        RandomCutForestModeConfig randomCutForestModeConfig = new RandomCutForestModeConfig();
        setField(RandomCutForestModeConfig.class, randomCutForestModeConfig, "type", UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> randomCutForestModeConfig.getType());
    }
}
