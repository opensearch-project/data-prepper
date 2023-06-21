/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileBufferFactory implements BufferFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBufferFactory.class);
    public static final String PREFIX = "local";
    public static final String SUFFIX = ".log";
    @Override
    public Buffer getBuffer() {
       //TODO: implement
        return null;
    }
}
