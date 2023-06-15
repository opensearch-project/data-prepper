/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScanOptionsTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @ParameterizedTest
    @MethodSource("validGlobalTimeRangeOptions")
    public void s3scan_options_with_valid_global_time_range_build_success(
            LocalDateTime startDateTime, LocalDateTime endDateTime, Duration range,
            LocalDateTime useStartDateTime, LocalDateTime useEndDateTime) throws JsonProcessingException {
        final String buketOptionYaml = "name: bucket_name";
        final ScanOptions scanOptions = ScanOptions.builder()
                .setStartDateTime(startDateTime)
                .setEndDateTime(endDateTime)
                .setRange(range)
                .setBucketOption(objectMapper.readValue(buketOptionYaml, S3ScanBucketOption.class))
                .build();

        assertThat(scanOptions.getUseStartDateTime(), equalTo(useStartDateTime));
        assertThat(scanOptions.getUseEndDateTime(), equalTo(useEndDateTime));
        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
    }

    @ParameterizedTest
    @MethodSource("invalidGlobalTimeRangeOptions")
    public void s3scan_options_with_invalid_global_time_range_throws_exception_when_build(
            LocalDateTime startDateTime, LocalDateTime endDateTime, Duration range) {
        final String buketOptionYaml = "name: bucket_name";
        assertThrows(IllegalArgumentException.class, () -> ScanOptions.builder()
                .setStartDateTime(startDateTime)
                .setEndDateTime(endDateTime)
                .setRange(range)
                .setBucketOption(objectMapper.readValue(buketOptionYaml, S3ScanBucketOption.class))
                .build());
    }


    @ParameterizedTest
    @MethodSource("validBucketTimeRangeOptions")
    public void s3scan_options_with_valid_bucket_time_range_build_success(
            String bucketOptionYaml, LocalDateTime useStartDateTime, LocalDateTime useEndDateTime) throws JsonProcessingException {
        final ScanOptions scanOptions = ScanOptions.builder()
                .setBucketOption(objectMapper.readValue(bucketOptionYaml, S3ScanBucketOption.class))
                .build();

        assertThat(scanOptions.getUseStartDateTime(), equalTo(useStartDateTime));
        assertThat(scanOptions.getUseEndDateTime(), equalTo(useEndDateTime));
        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
    }

    @Test
    public void bucket_time_range_options_overrides_global_time_range_options() throws JsonProcessingException {
        final String bucketOptionYaml = "name: bucket_name\n" +
                "start_time: 2023-01-21T18:00:00\n" +
                "end_time: 2023-01-24T18:00:00";
        final ScanOptions scanOptions = ScanOptions.builder()
                .setStartDateTime(LocalDateTime.parse("2022-05-10T18:00:00"))
                .setEndDateTime(LocalDateTime.parse("2022-05-24T18:00:00"))
                .setBucketOption(objectMapper.readValue(bucketOptionYaml, S3ScanBucketOption.class))
                .build();

        assertThat(scanOptions.getUseStartDateTime(), equalTo(LocalDateTime.parse("2023-01-21T18:00:00")));
        assertThat(scanOptions.getUseEndDateTime(), equalTo(LocalDateTime.parse("2023-01-24T18:00:00")));
        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
    }
    
    private static Stream<Arguments> validGlobalTimeRangeOptions() {
        return Stream.of(
                Arguments.of(
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), null,
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), null, Duration.parse("P3D"),
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(null, LocalDateTime.parse("2023-01-24T18:00:00"), Duration.parse("P3D"),
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(null, null, null, null, null)
        );
    }

    private static Stream<Arguments> invalidGlobalTimeRangeOptions() {
        return Stream.of(
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-04-21T18:00:00"),
                        Duration.parse("P90DT3H4M")),
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), null, null),
                Arguments.of(null, LocalDateTime.parse("2023-04-21T18:00:00"), null),
                Arguments.of(null, null, Duration.parse("P90DT3H4M"))
        );
    }

    private static Stream<Arguments> validBucketTimeRangeOptions() {
        final String bucket1 = "name: bucket_name\n" +
                "start_time: 2023-01-21T18:00:00\n" +
                "end_time: 2023-01-24T18:00:00";
        final String bucket2 = "name: bucket_name\n" +
                "start_time: 2023-01-21T18:00:00\n" +
                "range: P3D";
        final String bucket3 = "name: bucket_name\n" +
                "range: P3D\n" +
                "end_time: 2023-01-24T18:00:00";
        final String bucket4 = "name: bucket_name";
        return Stream.of(
                Arguments.of(bucket1, LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(bucket2, LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(bucket3, LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(bucket4, null, null)
        );
    }
    
}