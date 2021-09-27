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

package com.amazon.dataprepper.plugins.prepper.grok;

import com.amazon.dataprepper.model.record.Record;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(MockitoExtension.class)
public class GrokPrepperTests {

    PluginSetting pluginSetting;
    private GrokPrepper grokPrepper;

    @BeforeEach
    public void setup() {
        pluginSetting = new PluginSetting(
                "grok",
                null
                );
        pluginSetting.setPipelineName("grokPipeline");
        grokPrepper = new GrokPrepper(pluginSetting);
    }

    @AfterEach
    public void tearDown() {
      grokPrepper.shutdown();
    }

    @Test
    public void testGrokPrepper() {
        String testData = "{\"message\": \"127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] BEF25A72965 \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326\"}";
        Record<String> record = new Record<>(testData);

        List<Record<String>> grokkedRecords = (List<Record<String>>) grokPrepper.doExecute(Collections.singletonList(record));

        assertThat(grokkedRecords.size(), equalTo(1));
        assertThat(grokkedRecords.get(0), equalTo(record));
    }
}
