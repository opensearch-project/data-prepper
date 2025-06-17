/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.codec.JsonDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An implementation of {@link InputCodec} which parses JSON Objects for arrays.
 */
@DataPrepperPlugin(name = "json", pluginType = InputCodec.class, pluginConfigurationType = JsonInputCodecConfig.class)
public class JsonInputCodec extends JsonDecoder implements InputCodec {

    @DataPrepperPluginConstructor
    public JsonInputCodec(final JsonInputCodecConfig config) {
        super(Objects.requireNonNull(config).getKeyName(), config.getIncludeKeys(), config.getIncludeKeysMetadata(), config.getMaxEventLength());
    }

    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        parse(inputStream, null, eventConsumer);
    }
}
