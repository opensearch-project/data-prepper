/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import software.amazon.awssdk.services.s3.S3Client;

import java.util.function.Supplier;

public interface BufferFactory {
    Buffer getBuffer(S3Client s3Client, Supplier<String> bucketSupplier, Supplier<String> keySupplier);
}
