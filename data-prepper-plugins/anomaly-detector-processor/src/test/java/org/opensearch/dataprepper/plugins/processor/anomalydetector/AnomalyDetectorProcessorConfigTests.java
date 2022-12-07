/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class AnomalyDetectorProcessorConfigTests {
    @Test
    public void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig = new AnomalyDetectorProcessorConfig();
        List<String> keyList = new ArrayList<String>();
        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        keyList.add(key1);
        keyList.add(key2);
        setField(AnomalyDetectorProcessorConfig.class, anomalyDetectorProcessorConfig, "keys", keyList);
        assertThat(anomalyDetectorProcessorConfig.getKeys().get(0), equalTo(key1));
        assertThat(anomalyDetectorProcessorConfig.getKeys().get(1), equalTo(key2));
    }
}
