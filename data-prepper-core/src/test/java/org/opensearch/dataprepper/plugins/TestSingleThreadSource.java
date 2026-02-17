/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

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
