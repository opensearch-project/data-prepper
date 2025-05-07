/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

public class S3BucketAndPrefix {
    static final String S3_PREFIX = "s3://";

    private final String bucketName;
    private final String prefix;

    public S3BucketAndPrefix(String bucketName, String prefix) {
        this.bucketName = bucketName;
        this.prefix = prefix;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getPrefix() {
        return prefix;
    }

    public static S3BucketAndPrefix fromS3Uri(final String s3Uri) {
        if (s3Uri == null || !s3Uri.startsWith(S3_PREFIX)) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }

        final String withoutScheme = s3Uri.substring(S3_PREFIX.length());
        final int firstSlashIndex = withoutScheme.indexOf("/");

        final String bucketName;
        String prefix = "";

        if (firstSlashIndex == -1) {
            // No prefix, only bucket name
            bucketName = withoutScheme;
        } else {
            // Extract bucket and prefix
            bucketName = withoutScheme.substring(0, firstSlashIndex);
            prefix = withoutScheme.substring(firstSlashIndex + 1);
        }

        return new S3BucketAndPrefix(bucketName, prefix);
    }
}
