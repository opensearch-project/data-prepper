/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.OutputStream;

public class JsonOutputCodec implements OutputCodec {
    @Override
    public void start(OutputStream outputStream) {


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
