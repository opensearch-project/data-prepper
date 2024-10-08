/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "opentelemetry_logs", pluginType = InputCodec.class)
public class OTLPJsonLogsCodec extends OTLPJsonLogsDecoder implements InputCodec {
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        parse(inputStream, null, eventConsumer);
    }      
}