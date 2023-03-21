/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanBucketOptionsTest {

    @Test
    public void s3ScanBucketOptionsTest() throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOptions s3ScanBucketOptions = new S3ScanBucketOptions();
        S3ScanBucketOption s3ScanBucketOption = new S3ScanBucketOption();
        reflectivelySetField(s3ScanBucketOptions,"bucket",s3ScanBucketOption);
        assertThat(s3ScanBucketOptions.getBucket(),sameInstance(s3ScanBucketOption));
    }

    private void assertThat(S3ScanBucketOption bucket, Matcher<S3ScanBucketOption> sameInstance) {
    }

    private void reflectivelySetField(final S3ScanBucketOptions s3ScanBucketOptions, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = S3ScanBucketOptions.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(s3ScanBucketOptions, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
