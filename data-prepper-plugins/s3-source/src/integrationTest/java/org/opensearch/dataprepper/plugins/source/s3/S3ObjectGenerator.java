/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class S3ObjectGenerator {

    private static final int DEFAULT_BLOCK_SIZE = 8192;

    private final S3Client s3Client;
    private final String bucketName;

    S3ObjectGenerator(final S3Client s3Client, final String bucketName) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
    }

    void write(final int numberOfRecords, final String key, final RecordsGenerator objectGenerator, final boolean isCompressionEnabled) throws IOException {
        final File tempFile = File.createTempFile("s3-source-" + numberOfRecords + "-", null);
        tempFile.deleteOnExit();

        writeToFile(numberOfRecords, objectGenerator, isCompressionEnabled, tempFile);

        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3Client.putObject(putObjectRequest, tempFile.toPath());
    }

    private void writeToFile(final int numberOfRecords,
                             final RecordsGenerator recordsGenerator,
                             final boolean isCompressionEnabled,
                             final File file) throws IOException {
        if (isCompressionEnabled) {
            final File tempFile = File.createTempFile("s3-uncompressed-" + numberOfRecords + "-", null);
            tempFile.deleteOnExit();
            recordsGenerator.write(tempFile, numberOfRecords);

            compressGzip(tempFile, file);
        }
        else {
            recordsGenerator.write(file, numberOfRecords);
        }
    }

    private static void compressGzip(final File source, final File target) throws IOException {

        try (GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(target));
             final FileInputStream fis = new FileInputStream(source)) {

            // copy file
            byte[] buffer = new byte[DEFAULT_BLOCK_SIZE];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                gos.write(buffer, 0, len);
            }
        }

    }
}
