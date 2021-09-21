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

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Collections;
import java.util.List;

public class GrokPrepperTests {

    PluginSetting pluginSetting;
    public GrokPrepper grokPrepper;

    @Before
    public void setup() {
        pluginSetting = new PluginSetting(
                "grok_prepper",
                null
                );
        pluginSetting.setPipelineName("grokPipeline");
        grokPrepper = new GrokPrepper(pluginSetting);
    }

    @After
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
