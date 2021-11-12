/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;

@SingleThread
@DataPrepperPlugin(name = "test_sink_updated", pluginType = Source.class)
public class TestSingleThreadSource implements Source<Record<String>> {
    @Override
    public void start(Buffer<Record<String>> buffer) {

    }

    @Override
    public void stop() {

    }
}
