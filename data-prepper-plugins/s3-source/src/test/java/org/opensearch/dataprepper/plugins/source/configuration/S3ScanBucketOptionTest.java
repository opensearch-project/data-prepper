/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class S3ScanBucketOptionTest {
    @Test
    public void s3ScanBucketOptionTest() throws NoSuchFieldException, IllegalAccessException {
        final S3ScanBucketOption s3ScanBucketOption = new S3ScanBucketOption();
        final String bucketName = "my-bucket-1";
        final PluginModel plugInModel = mock(PluginModel.class);
        final List<String> stringList = Arrays.asList("file1.csv", "file2.csv");
        final S3SelectOptions s3SelectOptions = mock(S3SelectOptions.class);
        reflectivelySetField(s3ScanBucketOption,"name",bucketName);
        reflectivelySetField(s3ScanBucketOption,"codec",plugInModel);
        reflectivelySetField(s3ScanBucketOption,"s3SelectOptions",s3SelectOptions);
        reflectivelySetField(s3ScanBucketOption,"keyPath", stringList);
        reflectivelySetField(s3ScanBucketOption,"compression", CompressionOption.NONE);

        assertThat(s3ScanBucketOption.getName(), notNullValue());
        assertThat(s3ScanBucketOption.getCodec(), notNullValue());
        assertThat(s3ScanBucketOption.getS3SelectOptions(), notNullValue());
        assertThat(s3ScanBucketOption.getKeyPath(), notNullValue());
        assertThat(s3ScanBucketOption.getName(),equalTo(bucketName));
        assertThat(s3ScanBucketOption.getCodec(),sameInstance(plugInModel));
        assertThat(s3ScanBucketOption.getS3SelectOptions(),sameInstance(s3SelectOptions));
        assertThat(s3ScanBucketOption.getKeyPath(),sameInstance(stringList));
        assertThat(s3ScanBucketOption.getCompression(),sameInstance(CompressionOption.NONE));
    }
    private void reflectivelySetField(final S3ScanBucketOption s3ScanBucketOption, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = S3ScanBucketOption.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(s3ScanBucketOption, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
