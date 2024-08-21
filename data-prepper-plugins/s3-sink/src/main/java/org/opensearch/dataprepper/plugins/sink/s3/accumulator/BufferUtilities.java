/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

class BufferUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(BufferUtilities.class);

    static final String ACCESS_DENIED = "Access Denied";
    static final String INVALID_BUCKET = "The specified bucket is not valid";

    static CompletableFuture<PutObjectResponse> putObjectOrSendToDefaultBucket(final S3AsyncClient s3Client,
                                                                               final AsyncRequestBody requestBody,
                                                                               final Consumer<Boolean> runOnCompletion,
                                                                               final Consumer<Throwable> runOnFailure,
                                                                               final String objectKey,
                                                                               final String targetBucket,
                                                                               final String defaultBucket,
                                                                               final Map<String, String> objectMetadata,
                                                                               final BucketOwnerProvider bucketOwnerProvider) {

        final boolean[] defaultBucketAttempted = new boolean[1];
        PutObjectRequest.Builder builder =  PutObjectRequest.builder()
                .bucket(targetBucket)
                .key(objectKey)
                .expectedBucketOwner(bucketOwnerProvider.getBucketOwner(targetBucket).orElse(null));
        if (objectMetadata != null) {
            builder = builder.metadata(objectMetadata);
        }
        return s3Client.putObject(builder.build(), requestBody)
                .handle((result, ex) -> {
                    if (ex != null) {
                        runOnFailure.accept(ex);

                        if (defaultBucket != null &&
                                (ex instanceof NoSuchBucketException || ex.getCause() instanceof NoSuchBucketException || ex.getMessage().contains(ACCESS_DENIED) || ex.getMessage().contains(INVALID_BUCKET))) {
                            LOG.warn("Bucket {} could not be accessed, attempting to send to default_bucket {}", targetBucket, defaultBucket);
                            defaultBucketAttempted[0] = true;
                            return s3Client.putObject(
                                    PutObjectRequest.builder()
                                            .bucket(defaultBucket)
                                            .key(objectKey)
                                            .expectedBucketOwner(bucketOwnerProvider.getBucketOwner(defaultBucket).orElse(null))
                                            .build(),
                                    requestBody);
                        } else {
                            runOnCompletion.accept(false);
                            return CompletableFuture.completedFuture(result);
                        }
                    }

                    runOnCompletion.accept(true);
                    return CompletableFuture.completedFuture(result);
                })
                .thenCompose(Function.identity())
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        runOnFailure.accept(ex);
                    }

                    if (defaultBucketAttempted[0]) {
                        runOnCompletion.accept(ex == null);
                    }
                });
    }
}
