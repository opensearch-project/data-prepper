/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.source.codec.Codec;

import java.io.IOException;
import java.io.OutputStream;

interface RecordsGenerator {
    void write(int numberOfRecords, OutputStream outputStream) throws IOException;

    Codec getCodec();

    String getFileExtension();

    void assertEventIsCorrect(Event event);
    String getQueryStatement();
}
