/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.codec;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;

/**
 * An implementation of {@link Codec} which serializes to JSON.
 */
@DataPrepperPlugin(name = "ndjson", pluginType = Codec.class)
public class JsonCodec implements Codec {
    /**
     * Generates a serialized json string of the Event
     */
    @Override
    public String parse(Event event) throws IOException {
        Objects.requireNonNull(event);
        return event.toJsonString();
    }
}