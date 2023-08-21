/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

public class LocalFileBufferFactory implements BufferFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBufferFactory.class);
    public static final String PREFIX = "local";
    public static final String SUFFIX = ".log";

    @Override
    public Buffer getBuffer(S3Client s3Client, Supplier<String> bucketSupplier, Supplier<String> keySupplier) {
        File tempFile = null;
        Buffer localfileBuffer = null;
        try {
            tempFile = File.createTempFile(PREFIX, SUFFIX);
            localfileBuffer = new LocalFileBuffer(tempFile, s3Client, bucketSupplier, keySupplier);
        } catch (IOException e) {
            LOG.error("Unable to create temp file ", e);
        }
        return localfileBuffer;
    }
}
