/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.InputStream;

public class S3ObjectReader {

    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectReader.class);

    private final S3Client s3Client;

    public S3ObjectReader(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public InputStream readFile(String bucketName, String s3Key) {
        LOG.debug("Read file from s3://{}/{}", bucketName, s3Key);

        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        return s3Client.getObject(objectRequest);
    }

}
