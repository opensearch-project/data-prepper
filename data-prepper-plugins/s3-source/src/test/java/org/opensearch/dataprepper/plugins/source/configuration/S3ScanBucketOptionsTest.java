/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanBucketOptionsTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void s3_scan_bucket_option_yaml_configuration_test() throws JsonProcessingException {

        final String bucketOptionsYaml = "          bucket:\n" +
                "              name: test-s3-source-test-output\n" +
                "              key_prefix:\n" +
                "                include:\n" +
                "                  - bucket2\n" +
                "                exclude_suffix:\n" +
                "                  - .jpeg";
        final S3ScanBucketOptions s3ScanBucketOptions = objectMapper.readValue(bucketOptionsYaml, S3ScanBucketOptions.class);
        assertThat(s3ScanBucketOptions.getS3ScanBucketOption(),instanceOf(S3ScanBucketOption.class));
        assertThat(s3ScanBucketOptions.getS3ScanBucketOption().getName(),equalTo("test-s3-source-test-output"));
        assertThat(s3ScanBucketOptions.getS3ScanBucketOption().getkeyPrefix().getS3scanIncludeOptions(),instanceOf(List.class));
        assertThat(s3ScanBucketOptions.getS3ScanBucketOption().getkeyPrefix().getS3scanIncludeOptions().get(0), Matchers.equalTo("bucket2"));
        assertThat(s3ScanBucketOptions.getS3ScanBucketOption().getkeyPrefix().getS3ScanExcludeSuffixOptions(),instanceOf(List.class));
        assertThat(s3ScanBucketOptions.getS3ScanBucketOption().getkeyPrefix().getS3ScanExcludeSuffixOptions().get(0), Matchers.equalTo(".jpeg"));
    }
}
