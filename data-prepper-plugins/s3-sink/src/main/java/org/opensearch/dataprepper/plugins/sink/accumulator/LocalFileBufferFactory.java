/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;

public class LocalFileBufferFactory implements BufferFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBufferFactory.class);
    public static final String PREFIX = "local";
    public static final String SUFFIX = ".log";
    @Override
    public Buffer getBuffer() {
        File tempFile = null;
        Buffer localfileBuffer = null;
        try {
            tempFile = File.createTempFile(PREFIX, SUFFIX);
            localfileBuffer = new LocalFileBuffer(tempFile);
        } catch (IOException e) {
            LOG.error("Unable to create temp file ", e);
        }
        return localfileBuffer;
    }
}
