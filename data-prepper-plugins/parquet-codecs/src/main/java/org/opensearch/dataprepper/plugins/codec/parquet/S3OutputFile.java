/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;


public class S3OutputFile implements OutputFile {

    private S3Client s3Client;

    private String bucketName;

    private String key;


    public S3OutputFile(final S3Client s3Client, final String bucketName,
                        final String key) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new S3OutputStream(s3Client, bucketName, key);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return null;
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

}
