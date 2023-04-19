/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Interface for building buffer types.
 */
public interface BufferType {

    public static final Logger LOG = LoggerFactory.getLogger(BufferType.class);

    /**
     * Upload accumulated data to amazon s3 and perform retry in-case any issue occurred, based on
     * max_upload_retries configuration.
     * 
     * @param s3SinkConfig
     * @param s3Client
     * @param requestBody
     * @return
     * @throws InterruptedException
     */
    public default boolean uploadToAmazonS3(S3SinkConfig s3SinkConfig, S3Client s3Client, RequestBody requestBody)
            throws InterruptedException {

        final String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        final String namePattern = ObjectKey.objectFileName(s3SinkConfig);
        final String bucketName = s3SinkConfig.getBucketOptions().getBucketName();

        final String key = (pathPrefix != null && !pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
        boolean isFileUploadedToS3 = Boolean.FALSE;
        int retryCount = s3SinkConfig.getMaxUploadRetries();
        do {
            try {
                PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
                s3Client.putObject(request, requestBody);
                isFileUploadedToS3 = Boolean.TRUE;
            } catch (AwsServiceException | SdkClientException e) {
                LOG.error("Exception occurred while upload file {} to amazon s3 bucket. Retry count  : {} exception:",
                        namePattern, retryCount, e);
                --retryCount;
                if (retryCount == 0) {
                    return isFileUploadedToS3;
                }
                Thread.sleep(5000);
            }
        } while (!isFileUploadedToS3);
        return isFileUploadedToS3;
    }
}