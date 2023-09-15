/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;

import java.io.File;
import java.io.IOException;

interface RecordsGenerator {
    void write(File file, int numberOfRecords) throws IOException;

    InputCodec getCodec();

    String getFileExtension();

    void assertEventIsCorrect(Event event);
    String getS3SelectExpression();

    boolean canCompress();
}
