/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.OutputStream;
import java.util.Objects;

public class AvroOutputCodec implements OutputCodec {

    @Override
    public void start(OutputStream outputStream) {
        Objects.requireNonNull(outputStream);


    }

    @Override
    public void complete(OutputStream outputStream) {

    }

    @Override
    public void writeEvent(Event event, OutputStream outputStream) {

    }

    @Override
    public String getExtension() {
        return null;
    }
}
