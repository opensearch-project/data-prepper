/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanScanOptionsTest {

    @Test
    public void s3ScanScanOptionsTest() throws NoSuchFieldException, IllegalAccessException {
        final List<S3ScanBucketOptions> s3ScanBucketOptions = Arrays.asList(new S3ScanBucketOptions());
        S3ScanScanOptions s3ScanScanOptions = new S3ScanScanOptions();
        ReflectivelySetField.setField(S3ScanScanOptions.class,s3ScanScanOptions,"startTime","2023-03-07T10:00:00");
        ReflectivelySetField.setField(S3ScanScanOptions.class,s3ScanScanOptions,"range","2d");
        ReflectivelySetField.setField(S3ScanScanOptions.class,s3ScanScanOptions,"buckets",s3ScanBucketOptions);

        assertThat(s3ScanScanOptions.getStartTime(),notNullValue());
        assertThat(s3ScanScanOptions.getRange(),notNullValue());
        assertThat(s3ScanScanOptions.getBuckets(),notNullValue());
    }
}
