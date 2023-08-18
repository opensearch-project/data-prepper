/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.codec.parquet.S3OutputStream;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.function.Supplier;

public class MultipartBufferFactory implements BufferFactory {
    @Override
    public Buffer getBuffer(S3Client s3Client, Supplier<String> bucketSupplier, Supplier<String> keySupplier) {
        return new MultipartBuffer(new S3OutputStream(s3Client, bucketSupplier, keySupplier));
    }
}
