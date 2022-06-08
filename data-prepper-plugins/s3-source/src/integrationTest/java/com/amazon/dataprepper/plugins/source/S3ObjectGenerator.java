/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class S3ObjectGenerator {
    private final S3Client s3Client;
    private final String bucketName;

    S3ObjectGenerator(final S3Client s3Client, final String bucketName) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }

    void write(final int numberOfRecords, final String key, final RecordsGenerator objectGenerator) throws IOException {
        final File tempFile = File.createTempFile("s3-source-" + numberOfRecords + "-", null);

        try {
            try (final OutputStream outputStream = new FileOutputStream(tempFile)) {

                objectGenerator.write(numberOfRecords, outputStream);
                outputStream.flush();
            }

            final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.putObject(putObjectRequest, tempFile.toPath());
        } finally {
            tempFile.delete();
        }
    }
}
