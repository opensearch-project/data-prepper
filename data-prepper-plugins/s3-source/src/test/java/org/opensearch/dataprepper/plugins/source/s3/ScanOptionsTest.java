/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOption;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class ScanOptionsTest {

    @ParameterizedTest
    @MethodSource("validGlobalTimeRangeOptions")
    void s3scan_options_with_valid_global_time_range_build_success(
            LocalDateTime startDateTime, LocalDateTime endDateTime, Duration range,
            LocalDateTime useStartDateTime, LocalDateTime useEndDateTime) throws NoSuchFieldException, IllegalAccessException {
        S3ScanBucketOption bucketOption = new S3ScanBucketOption();
        setField(S3ScanBucketOption.class, bucketOption, "name", "bucket_name");
        final ScanOptions scanOptions = ScanOptions.builder()
                .setStartDateTime(startDateTime)
                .setEndDateTime(endDateTime)
                .setRange(range)
                .setBucketOption(bucketOption)
                .build();

        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
        validateStartAndEndTime(useStartDateTime, useEndDateTime, scanOptions);
    }

    @ParameterizedTest
    @MethodSource("validBucketTimeRangeOptions")
    void s3scan_options_with_valid_bucket_time_range_build_success(
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

        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
        validateStartAndEndTime(useStartDateTime, useEndDateTime, scanOptions);
    }

    @ParameterizedTest
    @MethodSource("validCombinedTimeRangeOptions")
    void s3scan_options_with_valid_combined_time_range_build_success(
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

        assertThat(scanOptions.getBucketOption(), instanceOf(S3ScanBucketOption.class));
        assertThat(scanOptions.getBucketOption().getName(), equalTo("bucket_name"));
        validateStartAndEndTime(useStartDateTime, useEndDateTime, scanOptions);
    }

    private static void validateStartAndEndTime(final LocalDateTime useStartDateTime,
                                                final LocalDateTime useEndDateTime,
                                                final ScanOptions scanOptions) {
        if (useStartDateTime != null) {
            assertThat(scanOptions.getUseStartDateTime(), lessThanOrEqualTo(useStartDateTime.plus(Duration.parse("PT5S"))));
            assertThat(scanOptions.getUseStartDateTime(), greaterThanOrEqualTo(useStartDateTime));
        }
        if (useEndDateTime != null) {
            assertThat(scanOptions.getUseEndDateTime(), lessThanOrEqualTo(useEndDateTime.plus(Duration.parse("PT5S"))));
            assertThat(scanOptions.getUseEndDateTime(), greaterThanOrEqualTo(useEndDateTime));
        }
    }

    private static Stream<Arguments> validGlobalTimeRangeOptions() {
        return Stream.of(
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), null,
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(null, null, Duration.parse("P90D"), LocalDateTime.now().minus(Duration.parse("P90D")), LocalDateTime.now()),
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), null, null, LocalDateTime.parse("2023-01-21T18:00:00"), null),
                Arguments.of(null, LocalDateTime.parse("2023-01-21T18:00:00"), null, null, LocalDateTime.parse("2023-01-21T18:00:00")),
                Arguments.of(null, null, null, null, null)
        );
    }

    private static Stream<Arguments> validBucketTimeRangeOptions() {
        return Stream.of(
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00"), null,
                        LocalDateTime.parse("2023-01-21T18:00:00"), LocalDateTime.parse("2023-01-24T18:00:00")),
                Arguments.of(null, null, Duration.parse("P90D"), LocalDateTime.now().minus(Duration.parse("P90D")), LocalDateTime.now()),
                Arguments.of(LocalDateTime.parse("2023-01-21T18:00:00"), null, null, LocalDateTime.parse("2023-01-21T18:00:00"), null),
                Arguments.of(null, LocalDateTime.parse("2023-01-21T18:00:00"), null, null, LocalDateTime.parse("2023-01-21T18:00:00")),
                Arguments.of(null, null, null, null, null)
        );
    }

    private static Stream<Arguments> validCombinedTimeRangeOptions() {
        return Stream.of(
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        LocalDateTime.parse("2023-08-21T18:00:00"), LocalDateTime.parse("2023-08-24T18:00:00"), null,
                        LocalDateTime.parse("2023-08-21T18:00:00"), LocalDateTime.parse("2023-08-24T18:00:00")),
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        LocalDateTime.parse("2023-08-21T18:00:00"), null, null,
                        LocalDateTime.parse("2023-08-21T18:00:00"), null),
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        null, LocalDateTime.parse("2023-08-24T18:00:00"), null,
                        null, LocalDateTime.parse("2023-08-24T18:00:00")),
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        null, null, null,
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00")),
                Arguments.of(
                        LocalDateTime.parse("2023-05-21T18:00:00"), LocalDateTime.parse("2023-05-24T18:00:00"), null,
                        null, null, Duration.parse("P90D"),
                        LocalDateTime.now().minus(Duration.parse("P90D")), LocalDateTime.now()),
                Arguments.of(
                        null, null, Duration.parse("P30D"),
                        null, null, Duration.parse("P90D"),
                        LocalDateTime.now().minus(Duration.parse("P90D")), LocalDateTime.now()),
                Arguments.of(
                        null, null, Duration.parse("P30D"),
                        null, null, null,
                        LocalDateTime.now().minus(Duration.parse("P30D")), LocalDateTime.now())
        );
    }
}
