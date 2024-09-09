/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanScanOptionsTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void s3scan_options_test_with_scan_yaml_configuration_test() throws JsonProcessingException {
        final String scanYaml = "        start_time: 2023-01-21T18:00:00\n" +
                "        end_time: 2023-04-21T18:00:00\n" +
                "        buckets:\n" +
                "          - bucket:\n" +
                "              name: test-s3-source-test-output\n" +
                "              filter:\n" +
                "                include_prefix:\n" +
                "                  - bucket2\n" +
                "                exclude_suffix:\n" +
                "                  - .jpeg";
        final S3ScanScanOptions s3ScanScanOptions = objectMapper.readValue(scanYaml, S3ScanScanOptions.class);
        assertThat(s3ScanScanOptions.getStartTime(),equalTo(LocalDateTime.parse("2023-01-21T18:00:00")));
        assertThat(s3ScanScanOptions.getEndTime(),equalTo(LocalDateTime.parse("2023-04-21T18:00:00")));
        assertThat(s3ScanScanOptions.getBuckets(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getName(),equalTo("test-s3-source-test-output"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3ScanExcludeSuffixOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions().get(0),
                equalTo("bucket2"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3ScanExcludeSuffixOptions().get(0),
                equalTo(".jpeg"));
    }

    @Test
    public void s3scan_options_with_scheduled_scan_does_not_allow_end_time() throws JsonProcessingException {
        final String scanYaml = "        start_time: 2023-01-21T18:00:00\n" +
                "        end_time: 2023-04-21T18:00:00\n" +
                "        scheduling: \n" +
                "          count: 1\n" +
                "        buckets:\n" +
                "          - bucket:\n" +
                "              name: test-s3-source-test-output\n" +
                "              filter:\n" +
                "                include_prefix:\n" +
                "                  - bucket2\n" +
                "                exclude_suffix:\n" +
                "                  - .jpeg";
        final S3ScanScanOptions s3ScanScanOptions = objectMapper.readValue(scanYaml, S3ScanScanOptions.class);
        assertThat(s3ScanScanOptions.getStartTime(),equalTo(LocalDateTime.parse("2023-01-21T18:00:00")));
        assertThat(s3ScanScanOptions.getEndTime(),equalTo(LocalDateTime.parse("2023-04-21T18:00:00")));
        assertThat(s3ScanScanOptions.getBuckets(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getName(),equalTo("test-s3-source-test-output"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3ScanExcludeSuffixOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions().get(0),
                equalTo("bucket2"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3ScanExcludeSuffixOptions().get(0),
                equalTo(".jpeg"));

        assertThat(s3ScanScanOptions.hasValidTimeOptionsWithScheduling(), equalTo(false));
    }

    @Test
    public void s3scan_options_with_scheduled_scan_allows_start_time() throws JsonProcessingException {
        final String scanYaml = "        start_time: 2023-01-21T18:00:00\n" +
                "        scheduling: \n" +
                "          count: 1\n" +
                "        buckets:\n" +
                "          - bucket:\n" +
                "              name: test-s3-source-test-output\n" +
                "              filter:\n" +
                "                include_prefix:\n" +
                "                  - bucket2\n" +
                "                exclude_suffix:\n" +
                "                  - .jpeg";
        final S3ScanScanOptions s3ScanScanOptions = objectMapper.readValue(scanYaml, S3ScanScanOptions.class);
        assertThat(s3ScanScanOptions.getStartTime(),equalTo(LocalDateTime.parse("2023-01-21T18:00:00")));
        assertThat(s3ScanScanOptions.getBuckets(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getName(),equalTo("test-s3-source-test-output"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3ScanExcludeSuffixOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3scanIncludePrefixOptions().get(0),
                equalTo("bucket2"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getS3ScanFilter().getS3ScanExcludeSuffixOptions().get(0),
                equalTo(".jpeg"));

        assertThat(s3ScanScanOptions.hasValidTimeOptionsWithScheduling(), equalTo(true));
    }
}
