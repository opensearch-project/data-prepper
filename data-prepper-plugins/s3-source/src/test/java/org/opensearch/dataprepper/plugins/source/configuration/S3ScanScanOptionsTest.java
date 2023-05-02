/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanScanOptionsTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void s3scan_options_test_with_scan_yaml_configuration_test() throws JsonProcessingException {
        final String scanYaml = "        start_time: 2023-01-21T18:00:00\n" +
                "        range: P90DT3H4M\n" +
                "        end_time: 2023-04-21T18:00:00\n" +
                "        buckets:\n" +
                "          - bucket:\n" +
                "              name: test-s3-source-test-output\n" +
                "              key_prefix:\n" +
                "                include:\n" +
                "                  - bucket2\n" +
                "                exclude_suffix:\n" +
                "                  - .jpeg";
        final S3ScanScanOptions s3ScanScanOptions = objectMapper.readValue(scanYaml, S3ScanScanOptions.class);
        assertThat(s3ScanScanOptions.getStartTime(),equalTo(LocalDateTime.parse("2023-01-21T18:00:00")));
        assertThat(s3ScanScanOptions.getEndTime(),equalTo(LocalDateTime.parse("2023-04-21T18:00:00")));
        assertThat(s3ScanScanOptions.getRange(),equalTo(Duration.parse("P90DT3H4M")));
        assertThat(s3ScanScanOptions.getBuckets(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getName(),equalTo("test-s3-source-test-output"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getkeyPrefix().getS3ScanExcludeSuffixOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getkeyPrefix().getS3scanIncludeOptions(),instanceOf(List.class));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getkeyPrefix().getS3scanIncludeOptions().get(0),
                equalTo("bucket2"));
        assertThat(s3ScanScanOptions.getBuckets().get(0).getS3ScanBucketOption().getkeyPrefix().getS3ScanExcludeSuffixOptions().get(0),
                equalTo(".jpeg"));
    }
}
