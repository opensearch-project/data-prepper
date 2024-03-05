/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanSchedulingOptions;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.source.s3.S3ScanPartitionCreationSupplier.LAST_SCAN_TIME;
import static org.opensearch.dataprepper.plugins.source.s3.S3ScanPartitionCreationSupplier.SCAN_COUNT;

@ExtendWith(MockitoExtension.class)
public class S3ScanPartitionCreationSupplierTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    private List<ScanOptions> scanOptionsList;

    private S3ScanSchedulingOptions schedulingOptions;

    @BeforeEach
    void setup() {
        scanOptionsList = new ArrayList<>();
    }


    private Function<Map<String, Object>, List<PartitionIdentifier>> createObjectUnderTest() {
        return new S3ScanPartitionCreationSupplier(s3Client, bucketOwnerProvider, scanOptionsList, schedulingOptions);
    }

    @Test
    void getNextPartition_supplier_without_scheduling_options_returns_expected_PartitionIdentifiers() {
        schedulingOptions = null;

        final String firstBucket = UUID.randomUUID().toString();
        final String secondBucket = UUID.randomUUID().toString();

        final Instant startTime = Instant.now();
        final Instant endTime = Instant.now().plus(3, ChronoUnit.MINUTES);


        final ScanOptions firstBucketScanOptions = mock(ScanOptions.class);
        final S3ScanBucketOption firstBucketScanBucketOption = mock(S3ScanBucketOption.class);
        given(firstBucketScanOptions.getBucketOption()).willReturn(firstBucketScanBucketOption);
        given(firstBucketScanBucketOption.getName()).willReturn(firstBucket);
        given(firstBucketScanOptions.getUseStartDateTime()).willReturn(LocalDateTime.ofInstant(startTime, ZoneId.systemDefault()));
        given(firstBucketScanOptions.getUseEndDateTime()).willReturn(LocalDateTime.ofInstant(endTime, ZoneId.systemDefault()));
        final S3ScanKeyPathOption firstBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(firstBucketScanBucketOption.getS3ScanFilter()).willReturn(firstBucketScanKeyPath);
        given(firstBucketScanKeyPath.getS3scanIncludePrefixOptions()).willReturn(List.of(UUID.randomUUID().toString()));
        given(firstBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(List.of(".invalid"));
        scanOptionsList.add(firstBucketScanOptions);

        final ScanOptions secondBucketScanOptions = mock(ScanOptions.class);
        final S3ScanBucketOption secondBucketScanBucketOption = mock(S3ScanBucketOption.class);
        given(secondBucketScanOptions.getBucketOption()).willReturn(secondBucketScanBucketOption);
        given(secondBucketScanBucketOption.getName()).willReturn(secondBucket);
        given(secondBucketScanOptions.getUseStartDateTime()).willReturn(LocalDateTime.ofInstant(startTime, ZoneId.systemDefault()));
        given(secondBucketScanOptions.getUseEndDateTime()).willReturn(LocalDateTime.ofInstant(endTime, ZoneId.systemDefault()));
        final S3ScanKeyPathOption secondBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(secondBucketScanBucketOption.getS3ScanFilter()).willReturn(secondBucketScanKeyPath);
        given(secondBucketScanKeyPath.getS3scanIncludePrefixOptions()).willReturn(null);
        given(secondBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(null);
        scanOptionsList.add(secondBucketScanOptions);

        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = createObjectUnderTest();

        final List<PartitionIdentifier> expectedPartitionIdentifiers = new ArrayList<>();

        final ListObjectsV2Response listObjectsResponse = mock(ListObjectsV2Response.class);
        final List<S3Object> s3ObjectsList = new ArrayList<>();

        final S3Object invalidFolderObject = mock(S3Object.class);
        given(invalidFolderObject.key()).willReturn("folder-key/");
        given(invalidFolderObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(invalidFolderObject);

        final S3Object invalidForFirstBucketSuffixObject = mock(S3Object.class);
        given(invalidForFirstBucketSuffixObject.key()).willReturn("test.invalid");
        given(invalidForFirstBucketSuffixObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(invalidForFirstBucketSuffixObject);
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + invalidForFirstBucketSuffixObject.key()).build());

        final S3Object invalidDueToLastModifiedOutsideOfStartEndObject = mock(S3Object.class);
        given(invalidDueToLastModifiedOutsideOfStartEndObject.key()).willReturn(UUID.randomUUID().toString());
        given(invalidDueToLastModifiedOutsideOfStartEndObject.lastModified()).willReturn(Instant.now().minus(3, ChronoUnit.MINUTES));
        s3ObjectsList.add(invalidDueToLastModifiedOutsideOfStartEndObject);

        final S3Object validObject = mock(S3Object.class);
        given(validObject.key()).willReturn("valid");
        given(validObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(validObject);
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + validObject.key()).build());
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + validObject.key()).build());

        given(listObjectsResponse.contents()).willReturn(s3ObjectsList);

        final ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        given(s3Client.listObjectsV2(listObjectsV2RequestArgumentCaptor.capture())).willReturn(listObjectsResponse);

        final Map<String, Object> globalStateMap = new HashMap<>();
        final List<PartitionIdentifier> resultingPartitions = partitionCreationSupplier.apply(globalStateMap);

        assertThat(globalStateMap, notNullValue());
        assertThat(globalStateMap.containsKey(SCAN_COUNT), equalTo(true));
        assertThat(globalStateMap.get(SCAN_COUNT), equalTo(1));

        globalStateMap.put(secondBucket, null);

        assertThat(partitionCreationSupplier.apply(globalStateMap), equalTo(Collections.emptyList()));

        assertThat(resultingPartitions, notNullValue());
        assertThat(resultingPartitions.size(), equalTo(expectedPartitionIdentifiers.size()));
        assertThat(resultingPartitions.stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiers.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));
    }

    @Test
    void getNextPartition_supplier_with_scheduling_options_returns_expected_PartitionIdentifiers() {
        schedulingOptions = mock(S3ScanSchedulingOptions.class);
        given(schedulingOptions.getInterval()).willReturn(Duration.ofMillis(0));
        given(schedulingOptions.getCount()).willReturn(2);

        final String firstBucket = "bucket-one";
        final String secondBucket = "bucket-two";

        final ScanOptions firstBucketScanOptions = mock(ScanOptions.class);
        final S3ScanBucketOption firstBucketScanBucketOption = mock(S3ScanBucketOption.class);
        given(firstBucketScanOptions.getBucketOption()).willReturn(firstBucketScanBucketOption);
        given(firstBucketScanBucketOption.getName()).willReturn(firstBucket);
        given(firstBucketScanOptions.getUseStartDateTime()).willReturn(null);
        given(firstBucketScanOptions.getUseEndDateTime()).willReturn(null);
        final S3ScanKeyPathOption firstBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(firstBucketScanBucketOption.getS3ScanFilter()).willReturn(firstBucketScanKeyPath);
        given(firstBucketScanKeyPath.getS3scanIncludePrefixOptions()).willReturn(List.of(UUID.randomUUID().toString()));
        given(firstBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(List.of(".invalid"));
        scanOptionsList.add(firstBucketScanOptions);

        final ScanOptions secondBucketScanOptions = mock(ScanOptions.class);
        final S3ScanBucketOption secondBucketScanBucketOption = mock(S3ScanBucketOption.class);
        given(secondBucketScanOptions.getBucketOption()).willReturn(secondBucketScanBucketOption);
        given(secondBucketScanBucketOption.getName()).willReturn(secondBucket);
        given(secondBucketScanOptions.getUseStartDateTime()).willReturn(null);
        given(secondBucketScanOptions.getUseEndDateTime()).willReturn(null);
        final S3ScanKeyPathOption secondBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(secondBucketScanBucketOption.getS3ScanFilter()).willReturn(secondBucketScanKeyPath);
        given(secondBucketScanKeyPath.getS3scanIncludePrefixOptions()).willReturn(null);
        given(secondBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(null);
        scanOptionsList.add(secondBucketScanOptions);

        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = createObjectUnderTest();

        final List<PartitionIdentifier> expectedPartitionIdentifiers = new ArrayList<>();

        final ListObjectsV2Response listObjectsResponse = mock(ListObjectsV2Response.class);
        final List<S3Object> s3ObjectsList = new ArrayList<>();

        final S3Object invalidFolderObject = mock(S3Object.class);
        given(invalidFolderObject.key()).willReturn("folder-key/");
        given(invalidFolderObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(invalidFolderObject);

        final S3Object invalidForFirstBucketSuffixObject = mock(S3Object.class);
        given(invalidForFirstBucketSuffixObject.key()).willReturn("test.invalid");
        given(invalidForFirstBucketSuffixObject.lastModified()).willReturn(Instant.now().minusSeconds(2));
        s3ObjectsList.add(invalidForFirstBucketSuffixObject);
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + invalidForFirstBucketSuffixObject.key()).build());

        final Instant mostRecentFirstScan = Instant.now().plusSeconds(1);
        final S3Object validObject = mock(S3Object.class);
        given(validObject.key()).willReturn("valid");
        given(validObject.lastModified()).willReturn(mostRecentFirstScan);
        s3ObjectsList.add(validObject);
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + validObject.key()).build());
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + validObject.key()).build());

        final S3Object secondScanObject = mock(S3Object.class);
        final Instant mostRecentSecondScan = Instant.now().plusSeconds(10);
        given(secondScanObject.key()).willReturn("second-scan");
        given(secondScanObject.lastModified()).willReturn(mostRecentSecondScan);

        final List<PartitionIdentifier> expectedPartitionIdentifiersSecondScan = new ArrayList<>();
        expectedPartitionIdentifiersSecondScan.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + secondScanObject.key()).build());
        expectedPartitionIdentifiersSecondScan.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + validObject.key()).build());
        expectedPartitionIdentifiersSecondScan.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + secondScanObject.key()).build());
        expectedPartitionIdentifiersSecondScan.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + validObject.key()).build());

        final List<S3Object> secondScanObjects = new ArrayList<>(s3ObjectsList);
        secondScanObjects.add(secondScanObject);
        given(listObjectsResponse.contents())
                .willReturn(s3ObjectsList)
                .willReturn(s3ObjectsList)
                .willReturn(s3ObjectsList)
                .willReturn(s3ObjectsList)
                .willReturn(secondScanObjects)
                .willReturn(secondScanObjects)
                .willReturn(secondScanObjects)
                .willReturn(secondScanObjects);

        final ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        given(s3Client.listObjectsV2(listObjectsV2RequestArgumentCaptor.capture())).willReturn(listObjectsResponse);

        final Map<String, Object> globalStateMap = new HashMap<>();
        final List<PartitionIdentifier> resultingPartitions = partitionCreationSupplier.apply(globalStateMap);

        assertThat(resultingPartitions, notNullValue());
        assertThat(resultingPartitions.size(), equalTo(expectedPartitionIdentifiers.size()));
        assertThat(resultingPartitions.stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiers.stream().map(PartitionIdentifier::getPartitionKey)
                        .map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(globalStateMap, notNullValue());
        assertThat(globalStateMap.containsKey(SCAN_COUNT), equalTo(true));
        assertThat(globalStateMap.get(SCAN_COUNT), equalTo(1));
        assertThat(globalStateMap.containsKey(firstBucket), equalTo(true));
        assertThat(globalStateMap.get(firstBucket), equalTo(mostRecentFirstScan.toString()));
        assertThat(globalStateMap.containsKey(secondBucket), equalTo(true));
        assertThat(globalStateMap.get(secondBucket), equalTo(mostRecentFirstScan.toString()));

        final List<PartitionIdentifier> secondScanPartitions = partitionCreationSupplier.apply(globalStateMap);
        assertThat(secondScanPartitions.size(), equalTo(expectedPartitionIdentifiersSecondScan.size()));
        assertThat(secondScanPartitions.stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersSecondScan.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(globalStateMap, notNullValue());
        assertThat(globalStateMap.containsKey(SCAN_COUNT), equalTo(true));
        assertThat(globalStateMap.get(SCAN_COUNT), equalTo(2));
        assertThat(globalStateMap.containsKey(firstBucket), equalTo(true));
        assertThat(globalStateMap.get(firstBucket), equalTo(mostRecentSecondScan.toString()));
        assertThat(globalStateMap.containsKey(secondBucket), equalTo(true));
        assertThat(globalStateMap.get(secondBucket), equalTo(mostRecentSecondScan.toString()));
        assertThat(Instant.ofEpochMilli((Long) globalStateMap.get(LAST_SCAN_TIME)).isBefore(Instant.now()), equalTo(true));

        assertThat(partitionCreationSupplier.apply(globalStateMap), equalTo(Collections.emptyList()));

        verify(listObjectsResponse, times(8)).contents();
    }
}
