/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanBucketOptionsTest {

    @Test
    public void s3ScanBucketOptionsTest() throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOptions s3ScanBucketOptions = new S3ScanBucketOptions();
        S3ScanBucketOption s3ScanBucketOption = new S3ScanBucketOption();
        ReflectivelySetField.setField(S3ScanBucketOptions.class,s3ScanBucketOptions,"scanBucketOption",s3ScanBucketOption);
        assertThat(s3ScanBucketOptions.getS3ScanBucketOption(),sameInstance(s3ScanBucketOption));
    }
}
