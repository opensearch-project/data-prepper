/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.configuration.ServerSideEncryptionConfig;
import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Map;
import java.util.function.Supplier;
import java.util.function.Function;

public interface BufferFactory {
    Buffer getBuffer(S3AsyncClient s3Client,
                     Supplier<String> bucketSupplier,
                     Supplier<String> keySupplier,
                     String defaultBucket,
                     Function<Integer, Map<String, String>> metadataSupplier,
                     BucketOwnerProvider bucketOwnerProvider,
                     ServerSideEncryptionConfig serverSideEncryptionConfig);
}
