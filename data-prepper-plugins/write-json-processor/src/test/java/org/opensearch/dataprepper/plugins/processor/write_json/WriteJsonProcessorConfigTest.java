/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.write_json;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.UUID;

public class WriteJsonProcessorConfigTest {
    WriteJsonProcessorConfig writeJsonProcessorConfig;

    @BeforeEach
    void setup() {
        writeJsonProcessorConfig = new WriteJsonProcessorConfig();
    }

    @Test
    public void testDefaults() {
        assertThat(writeJsonProcessorConfig.getTarget(), equalTo(null));
    }

    @Test
    public void testParameterSets() throws Exception {
        String sourceKey = UUID.randomUUID().toString();
        String targetKey = UUID.randomUUID().toString();
        ReflectivelySetField.setField(WriteJsonProcessorConfig.class, writeJsonProcessorConfig, "source", sourceKey);
        ReflectivelySetField.setField(WriteJsonProcessorConfig.class, writeJsonProcessorConfig, "target", targetKey);
        assertThat(writeJsonProcessorConfig.getSource(), equalTo(sourceKey));
        assertThat(writeJsonProcessorConfig.getTarget(), equalTo(targetKey));
    }
}
