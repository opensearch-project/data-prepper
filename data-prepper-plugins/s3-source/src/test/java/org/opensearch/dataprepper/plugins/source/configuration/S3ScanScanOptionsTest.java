/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanScanOptionsTest {

    @Test
    public void s3ScanScanOptionsTest() throws NoSuchFieldException, IllegalAccessException {
        final List<S3ScanBucketOptions> s3ScanBucketOptions = Arrays.asList(new S3ScanBucketOptions());
        S3ScanScanOptions s3ScanScanOptions = new S3ScanScanOptions();
        reflectivelySetField(s3ScanScanOptions,"startTime","2023-03-07T10:00:00");
        reflectivelySetField(s3ScanScanOptions,"range","2d");
        reflectivelySetField(s3ScanScanOptions,"buckets",s3ScanBucketOptions);

        assertThat(s3ScanScanOptions.getStartTime(),notNullValue());
        assertThat(s3ScanScanOptions.getRange(),notNullValue());
        assertThat(s3ScanScanOptions.getBuckets(),notNullValue());
    }
    private void reflectivelySetField(final S3ScanScanOptions s3ScanScanOptions, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = S3ScanScanOptions.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(s3ScanScanOptions, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
