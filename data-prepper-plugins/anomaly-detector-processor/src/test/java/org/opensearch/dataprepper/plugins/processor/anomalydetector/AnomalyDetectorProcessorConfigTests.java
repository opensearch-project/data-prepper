/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessorConfig.DEFAULT_MODE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessorConfig.DEFAULT_SHINGLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessorConfig.DEFAULT_SAMPLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessorConfig.DEFAULT_TIME_DECAY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessorConfig.MAX_SHINGLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessorConfig.MAX_SAMPLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessorConfig.MAX_TIME_DECAY;

public class AnomalyDetectorProcessorConfigTests {
    @Test
    public void testDefault() {
        final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig = new AnomalyDetectorProcessorConfig();
        assertThat(anomalyDetectorProcessorConfig.getMode(), equalTo(DEFAULT_MODE));
        assertThat(anomalyDetectorProcessorConfig.getShingleSize(), equalTo(DEFAULT_SHINGLE_SIZE));
        assertThat(anomalyDetectorProcessorConfig.getSampleSize(), equalTo(DEFAULT_SAMPLE_SIZE));
        assertThat(anomalyDetectorProcessorConfig.getTimeDecay(), equalTo(DEFAULT_TIME_DECAY));
    }

    @Test
    public void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig = new AnomalyDetectorProcessorConfig();
        List<String> keys = new ArrayList<String>(Collections.singleton("key1"));
        final int testShingleSize = 10;
        final int testSampleSize = 500;
        final double testTimeDecay = 0.44;
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "keys", keys);
        assertThat(anomalyDetectorProcessorConfig.getKeys(), equalTo(keys));
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "shingleSize", testShingleSize);
        assertThat(anomalyDetectorProcessorConfig.getShingleSize(), equalTo(testShingleSize));
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "sampleSize", testSampleSize);
        assertThat(anomalyDetectorProcessorConfig.getSampleSize(), equalTo(testSampleSize));
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "timeDecay", testTimeDecay);
        assertThat(anomalyDetectorProcessorConfig.getTimeDecay(), equalTo(testTimeDecay));
    }

    @Test
    public void testInvalidConfigMode() throws NoSuchFieldException, IllegalAccessException {
        AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig = new AnomalyDetectorProcessorConfig();
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "mode", UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> anomalyDetectorProcessorConfig.getMode());
    }

    @Test
    public void testInvalidConfigShingleSize() throws NoSuchFieldException, IllegalAccessException {
        AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig = new AnomalyDetectorProcessorConfig();
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "shingleSize", MAX_SHINGLE_SIZE+1);
        assertThrows(IllegalArgumentException.class, () -> anomalyDetectorProcessorConfig.getShingleSize());
    }

    @Test
    public void testValidConfigSampleSize() throws NoSuchFieldException, IllegalAccessException {
        AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig = new AnomalyDetectorProcessorConfig();
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "sampleSize", MAX_SAMPLE_SIZE+1);
        assertThrows(IllegalArgumentException.class, () -> anomalyDetectorProcessorConfig.getSampleSize());
    }

    @Test
    public void testValidConfigMode() throws NoSuchFieldException, IllegalAccessException {
        AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig = new AnomalyDetectorProcessorConfig();
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "timeDecay", MAX_TIME_DECAY+1);
        assertThrows(IllegalArgumentException.class, () -> anomalyDetectorProcessorConfig.getTimeDecay());
    }
}
