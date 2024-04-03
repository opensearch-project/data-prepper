/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class BufferUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(BufferUtilities.class);

    static final String ACCESS_DENIED = "Access Denied";

    static void putObjectOrSendToDefaultBucket(final S3Client s3Client,
                                               final RequestBody requestBody,
                                               final String objectKey,
                                               final String targetBucket,
                                               final String defaultBucket) {
        try {
            s3Client.putObject(
                    PutObjectRequest.builder().bucket(targetBucket).key(objectKey).build(),
                    requestBody);
        } catch (final S3Exception e) {
            if (defaultBucket != null && (e instanceof NoSuchBucketException || e.getMessage().contains(ACCESS_DENIED))) {
                LOG.warn("Bucket {} could not be accessed, attempting to send to default_bucket {}", targetBucket, defaultBucket);
                s3Client.putObject(
                        PutObjectRequest.builder().bucket(defaultBucket).key(objectKey).build(),
                        requestBody);
            } else {
                throw e;
            }
        }
    }
}
