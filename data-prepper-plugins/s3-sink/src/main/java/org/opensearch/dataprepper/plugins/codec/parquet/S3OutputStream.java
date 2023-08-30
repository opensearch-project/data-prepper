/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;


import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class S3OutputStream extends PositionOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

    /**
     * Default chunk size is 10MB
     */
    protected static final int BUFFER_SIZE = 10 * 1024 * 1024;

    /**
     * The bucket-name on Amazon S3
     */
    private final String bucket;

    /**
     * The key (path) name within the bucket
     */
    private final String key;

    /**
     * The temporary buffer used for storing the chunks
     */
    private final byte[] buf;

    private final S3Client s3Client;
    /**
     * Collection of the etags for the parts that have been uploaded
     */
    private final List<String> etags;
    /**
     * The position in the buffer
     */
    private int position;
    /**
     * The unique id for this upload
     */
    private String uploadId;
    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    /**
     * Creates a new S3 OutputStream
     *
     * @param s3Client the AmazonS3 client
     * @param bucketSupplier  name of the bucket
     * @param keySupplier     path within the bucket
     */
    public S3OutputStream(final S3Client s3Client, Supplier<String> bucketSupplier, Supplier<String> keySupplier) {
        this.s3Client = s3Client;
        this.bucket = bucketSupplier.get();
        this.key = keySupplier.get();
        buf = new byte[BUFFER_SIZE];
        position = 0;
        etags = new ArrayList<>();
        open = true;
    }

    @Override
    public void write(int b) {
        assertOpen();
        if (position >= buf.length) {
            flushBufferAndRewind();
        }
        buf[position++] = (byte) b;
    }


    /**
     * Write an array to the S3 output stream.
     *
     * @param b the byte-array to append
     */
    @Override
    public void write(byte[] b) {
        write(b, 0, b.length);
    }


    /**
     * Writes an array to the S3 Output Stream
     *
     * @param byteArray the array to write
     * @param o         the offset into the array
     * @param l         the number of bytes to write
     */
    @Override
    public void write(byte[] byteArray, int o, int l) {
        assertOpen();
        int ofs = o;
        int len = l;
        int size;
        while (len > (size = buf.length - position)) {
            System.arraycopy(byteArray, ofs, buf, position, size);
            position += size;
            flushBufferAndRewind();
            ofs += size;
            len -= size;
        }
        System.arraycopy(byteArray, ofs, buf, position, len);
        position += len;
    }

    /**
     * Flushing is not available because the parts must be of the same size.
     */
    @Override
    public void flush() {
    }

    @Override
    public void close() {
        if (open) {
            open = false;
            possiblyStartMultipartUpload();
            if (position > 0) {
                uploadPart();
            }

            CompletedPart[] completedParts = new CompletedPart[etags.size()];
            for (int i = 0; i < etags.size(); i++) {
                completedParts[i] = CompletedPart.builder()
                        .eTag(etags.get(i))
                        .partNumber(i + 1)
                        .build();
            }

            LOG.debug("Completing S3 multipart upload with {} parts.", completedParts.length);

            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();
            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
        }
    }

    public String getKey() {
        return key;
    }

    private void assertOpen() {
        if (!open) {
            throw new IllegalStateException("Closed");
        }
    }

    private void flushBufferAndRewind() {
        possiblyStartMultipartUpload();
        uploadPart();
        position = 0;
    }

    private void possiblyStartMultipartUpload() {
        if (uploadId == null) {
            CreateMultipartUploadRequest uploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(uploadRequest);
            uploadId = multipartUpload.uploadId();

            LOG.debug("Created multipart upload {} bucket='{}',key='{}'.", uploadId, bucket, key);
        }
    }

    private void uploadPart() {
        int partNumber = etags.size() + 1;
        UploadPartRequest uploadRequest = UploadPartRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) position)
                .build();
        RequestBody requestBody = RequestBody.fromInputStream(new ByteArrayInputStream(buf, 0, position),
                position);

        LOG.debug("Writing {} bytes to S3 multipart part number {}.", buf.length, partNumber);

        UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadRequest, requestBody);
        etags.add(uploadPartResponse.eTag());
    }

    @Override
    public long getPos() throws IOException {
        return position + (long) etags.size() * (long) BUFFER_SIZE;
    }
}

