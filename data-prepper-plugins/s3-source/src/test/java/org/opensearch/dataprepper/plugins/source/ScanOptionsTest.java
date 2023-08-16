/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

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
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class ScanOptionsTest {

    @ParameterizedTest
    @MethodSource("validGlobalTimeRangeOptions")
    public void s3scan_options_with_valid_global_time_range_build_success(
            LocalDateTime startDateTime, LocalDateTime endDateTime, Duration range,
            LocalDateTime useStartDateTime, LocalDateTime useEndDateTime) {
        final ScanOptions scanOptions = ScanOptions.builder()
                .setStartDateTime(startDateTime)
                .setEndDateTime(endDateTime)
                .setRange(range)
                .setBucketOption(new S3ScanBucketOption())
                .build();

        assertThat(scanOptions.getUseStartDateTime(), equalTo(useStartDateTime));
        assertThat(scanOptions.getUseEndDateTime(), equalTo(useEndDateTime));
        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
    }

    @ParameterizedTest
    @MethodSource("invalidTimeRangeOptions")
    public void s3scan_options_with_invalid_global_time_range_throws_exception_when_build(
            LocalDateTime startDateTime, LocalDateTime endDateTime, Duration range) throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOption bucketOption = new S3ScanBucketOption();
        setField(S3ScanBucketOption.class, bucketOption, "name", "bucket_name");

        assertThrows(IllegalArgumentException.class, () -> ScanOptions.builder()
                .setStartDateTime(startDateTime)
                .setEndDateTime(endDateTime)
                .setRange(range)
                .setBucketOption(bucketOption)
                .build());
    }

    @ParameterizedTest
    @MethodSource("validBucketTimeRangeOptions")
    public void s3scan_options_with_valid_bucket_time_range_build_success(
            LocalDateTime bucketStartDateTime, LocalDateTime bucketEndDateTime, Duration bucketRange,
            LocalDateTime useStartDateTime, LocalDateTime useEndDateTime) throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOption bucketOption = new S3ScanBucketOption();
        setField(S3ScanBucketOption.class, bucketOption, "name", "bucket_name");
        setField(S3ScanBucketOption.class, bucketOption, "startTime", bucketStartDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "endTime", bucketEndDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "range", bucketRange);
        final ScanOptions scanOptions = ScanOptions.builder()
                .setBucketOption(bucketOption)
                .build();

        assertThat(scanOptions.getUseStartDateTime(), equalTo(useStartDateTime));
        assertThat(scanOptions.getUseEndDateTime(), equalTo(useEndDateTime));
        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
    }

    @ParameterizedTest
    @MethodSource("invalidTimeRangeOptions")
    public void s3scan_options_with_invalid_bucket_time_range_throws_exception_when_build(
            LocalDateTime bucketStartDateTime, LocalDateTime bucketEndDateTime, Duration bucketRange
    ) throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOption bucketOption = new S3ScanBucketOption();
        setField(S3ScanBucketOption.class, bucketOption, "name", "bucket_name");
        setField(S3ScanBucketOption.class, bucketOption, "startTime", bucketStartDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "endTime", bucketEndDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "range", bucketRange);
        assertThrows(IllegalArgumentException.class, () -> ScanOptions.builder()
                .setBucketOption(bucketOption)
                .build());
    }

    @ParameterizedTest
    @MethodSource("validCombinedTimeRangeOptions")
    public void s3scan_options_with_valid_combined_time_range_build_success(
            LocalDateTime globalStartDateTime, LocalDateTime globeEndDateTime, Duration globalRange,
            LocalDateTime bucketStartDateTime, LocalDateTime bucketEndDateTime, Duration bucketRange,
            LocalDateTime useStartDateTime, LocalDateTime useEndDateTime) throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOption bucketOption = new S3ScanBucketOption();
        setField(S3ScanBucketOption.class, bucketOption, "name", "bucket_name");
        setField(S3ScanBucketOption.class, bucketOption, "startTime", bucketStartDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "endTime", bucketEndDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "range", bucketRange);
        final ScanOptions scanOptions = ScanOptions.builder()
                .setStartDateTime(globalStartDateTime)
                .setEndDateTime(globeEndDateTime)
                .setRange(globalRange)
                .setBucketOption(bucketOption)
                .build();

        assertThat(scanOptions.getUseStartDateTime(), equalTo(useStartDateTime));
        assertThat(scanOptions.getUseEndDateTime(), equalTo(useEndDateTime));
        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
    }

    @ParameterizedTest
    @MethodSource("invalidCombinedTimeRangeOptions")
    public void s3scan_options_with_invalid_combined_time_range_throws_exception_when_build(
            LocalDateTime globalStartDateTime, LocalDateTime globeEndDateTime, Duration globalRange,
            LocalDateTime bucketStartDateTime, LocalDateTime bucketEndDateTime, Duration bucketRange
    ) throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOption bucketOption = new S3ScanBucketOption();
        setField(S3ScanBucketOption.class, bucketOption, "name", "bucket_name");
        setField(S3ScanBucketOption.class, bucketOption, "startTime", bucketStartDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "endTime", bucketEndDateTime);
        setField(S3ScanBucketOption.class, bucketOption, "range", bucketRange);

        assertThrows(IllegalArgumentException.class, () -> ScanOptions.builder()
                .setStartDateTime(globalStartDateTime)
                .setEndDateTime(globeEndDateTime)
                .setRange(globalRange)
                .setBucketOption(bucketOption)
                .build());
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

    private static Stream<Arguments> invalidTimeRangeOptions() {
        return Stream.of(
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-04-21T18:00:00"),
                        Duration.parse("P90DT3H4M")),
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), null, null),
                Arguments.of(null, LocalDateTime.parse("2023-04-21T18:00:00"), null),
                Arguments.of(null, null, Duration.parse("P90DT3H4M"))
        );
    }

    private static Stream<Arguments> validBucketTimeRangeOptions() {
        return Stream.of(
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), null,
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), null, Duration.ofDays(3L),
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(null, LocalDateTime.parse("2023-01-24T18:00:00"), Duration.ofDays(3L),
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(null, null, null, null, null)
        );
    }

    private static Stream<Arguments> validCombinedTimeRangeOptions() {
        return Stream.of(
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), null, null,
                        null, LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00")),
                Arguments.of(
                        null, LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        LocalDateTime.parse("2023-05-21T18:00:00"), null, null,
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00")),
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), null, null,
                        null, null, Duration.ofDays(3L),
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00")),
                Arguments.of(
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), null,
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00")),
                Arguments.of(
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), null,
                        LocalDateTime.parse("2023-05-21T18:00:00"), null, Duration.ofDays(3L),
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"))
        );
    }

    private static Stream<Arguments> invalidCombinedTimeRangeOptions() {
        return Stream.of(
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), null, null,
                        LocalDateTime.parse("2023-05-24T18:00:00"), null, null),
                Arguments.of(
                        null, LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        null, LocalDateTime.parse("2023-05-21T18:00:00"), null),
                Arguments.of(
                        null, null, Duration.ofDays(3L),
                        null, null, Duration.ofDays(3L)),
                Arguments.of(
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), null,
                        null, null, Duration.ofDays(3L)),
                Arguments.of(
                        LocalDateTime.parse("2023-01-21T18:00:00"), null, Duration.ofDays(3L),
                        null, LocalDateTime.parse("2023-05-24T18:00:00"), null),
                Arguments.of(
                        null, LocalDateTime.parse("2023-01-24T18:00:00"), Duration.ofDays(3L),
                        LocalDateTime.parse("2023-05-21T18:00:00"), null, null),
                Arguments.of(
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), Duration.ofDays(3L),
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"), Duration.ofDays(3L))
        );
    }
}