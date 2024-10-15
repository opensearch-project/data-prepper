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
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.s3.configuration.FolderPartitioningOptions;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.s3.S3ScanPartitionCreationSupplier.LAST_SCAN_TIME;
import static org.opensearch.dataprepper.plugins.source.s3.S3ScanPartitionCreationSupplier.SCAN_COUNT;

@ExtendWith(MockitoExtension.class)
public class S3ScanPartitionCreationSupplierTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    @Mock
    private SourceCoordinator<S3SourceProgressState> sourceCoordinator;

    private List<ScanOptions> scanOptionsList;

    private S3ScanSchedulingOptions schedulingOptions;

    private FolderPartitioningOptions folderPartitioningOptions;

    @BeforeEach
    void setup() {
        scanOptionsList = new ArrayList<>();
        folderPartitioningOptions = null;
    }


    private Function<Map<String, Object>, List<PartitionIdentifier>> createObjectUnderTest() {
        return new S3ScanPartitionCreationSupplier(s3Client, bucketOwnerProvider, scanOptionsList, schedulingOptions, folderPartitioningOptions, sourceCoordinator);
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

        final List<PartitionIdentifier> expectedPartitionIdentifiersFirstBucket = new ArrayList<>();
        final List<PartitionIdentifier> expectedPartitionIdentifiersSecondBucket = new ArrayList<>();

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
        expectedPartitionIdentifiersSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + invalidForFirstBucketSuffixObject.key()).build());

        final S3Object invalidDueToLastModifiedOutsideOfStartEndObject = mock(S3Object.class);
        given(invalidDueToLastModifiedOutsideOfStartEndObject.key()).willReturn(UUID.randomUUID().toString());
        given(invalidDueToLastModifiedOutsideOfStartEndObject.lastModified()).willReturn(Instant.now().minus(3, ChronoUnit.MINUTES));
        s3ObjectsList.add(invalidDueToLastModifiedOutsideOfStartEndObject);

        final S3Object validObject = mock(S3Object.class);
        given(validObject.key()).willReturn("valid");
        given(validObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(validObject);
        expectedPartitionIdentifiersFirstBucket.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + validObject.key()).build());
        expectedPartitionIdentifiersSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + validObject.key()).build());

        given(listObjectsResponse.contents()).willReturn(s3ObjectsList);

        final ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        given(s3Client.listObjectsV2(listObjectsV2RequestArgumentCaptor.capture())).willReturn(listObjectsResponse);

        final ArgumentCaptor<List<PartitionIdentifier>> createPartitionsArgumentCaptor = ArgumentCaptor.forClass(List.class);
        doNothing().when(sourceCoordinator).createPartitions(createPartitionsArgumentCaptor.capture());


        final Map<String, Object> globalStateMap = new HashMap<>();
        final List<PartitionIdentifier> resultingPartitions = partitionCreationSupplier.apply(globalStateMap);

        assertThat(globalStateMap, notNullValue());
        assertThat(globalStateMap.containsKey(SCAN_COUNT), equalTo(true));
        assertThat(globalStateMap.get(SCAN_COUNT), equalTo(1));

        globalStateMap.put(secondBucket, null);

        final List<List<PartitionIdentifier>> createdPartitions = createPartitionsArgumentCaptor.getAllValues();
        assertThat(createdPartitions.size(), equalTo(2));
        assertThat(createdPartitions.get(0).size(), equalTo(expectedPartitionIdentifiersFirstBucket.size()));
        assertThat(createdPartitions.get(0).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersFirstBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(createdPartitions.get(1).size(), equalTo(expectedPartitionIdentifiersSecondBucket.size()));
        assertThat(createdPartitions.get(1).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersSecondBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(partitionCreationSupplier.apply(globalStateMap), equalTo(Collections.emptyList()));
        assertThat(resultingPartitions.isEmpty(), equalTo(true));

        verifyNoMoreInteractions(sourceCoordinator);
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

        final List<PartitionIdentifier> expectedPartitionIdentifiersFirstBucket = new ArrayList<>();
        final List<PartitionIdentifier> expectedPartitionIdentifiersSecondBucket = new ArrayList<>();

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
        expectedPartitionIdentifiersSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + invalidForFirstBucketSuffixObject.key()).build());

        final Instant mostRecentFirstScan = Instant.now().plusSeconds(2);
        final S3Object validObject = mock(S3Object.class);
        given(validObject.key()).willReturn("valid");
        given(validObject.lastModified()).willReturn(mostRecentFirstScan);
        s3ObjectsList.add(validObject);
        expectedPartitionIdentifiersFirstBucket.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + validObject.key()).build());
        expectedPartitionIdentifiersSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + validObject.key()).build());

        final S3Object secondScanObject = mock(S3Object.class);
        final Instant mostRecentSecondScan = Instant.now().plusSeconds(10);
        given(secondScanObject.key()).willReturn("second-scan");
        given(secondScanObject.lastModified()).willReturn(mostRecentSecondScan);

        final List<PartitionIdentifier> expectedPartitionIdentifiersSecondScanFirstBucket = new ArrayList<>();
        final List<PartitionIdentifier> expectedPartitionIdentifiersSecondScanSecondBucket = new ArrayList<>();
        expectedPartitionIdentifiersSecondScanFirstBucket.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + secondScanObject.key()).build());
        expectedPartitionIdentifiersSecondScanFirstBucket.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + validObject.key()).build());
        expectedPartitionIdentifiersSecondScanSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + secondScanObject.key()).build());
        expectedPartitionIdentifiersSecondScanSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + validObject.key()).build());

        final List<S3Object> secondScanObjects = new ArrayList<>(s3ObjectsList);
        secondScanObjects.add(secondScanObject);
        given(listObjectsResponse.contents())
                .willReturn(s3ObjectsList)
                .willReturn(s3ObjectsList)
                .willReturn(secondScanObjects)
                .willReturn(secondScanObjects);

        final ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        given(s3Client.listObjectsV2(listObjectsV2RequestArgumentCaptor.capture())).willReturn(listObjectsResponse);

        final ArgumentCaptor<List<PartitionIdentifier>> createPartitionsArgumentCaptor = ArgumentCaptor.forClass(List.class);
        doNothing().when(sourceCoordinator).createPartitions(createPartitionsArgumentCaptor.capture());

        final Map<String, Object> globalStateMap = new HashMap<>();

        final Instant beforeFirstScan = Instant.now();
        final List<PartitionIdentifier> resultingPartitions = partitionCreationSupplier.apply(globalStateMap);

        assertThat(resultingPartitions.isEmpty(), equalTo(true));

        assertThat(globalStateMap, notNullValue());
        assertThat(globalStateMap.containsKey(SCAN_COUNT), equalTo(true));
        assertThat(globalStateMap.get(SCAN_COUNT), equalTo(1));
        assertThat(globalStateMap.containsKey(firstBucket), equalTo(true));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(firstBucket)), lessThanOrEqualTo(mostRecentFirstScan));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(firstBucket)), greaterThanOrEqualTo(beforeFirstScan));
        assertThat(globalStateMap.containsKey(secondBucket), equalTo(true));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(secondBucket)), lessThanOrEqualTo(mostRecentFirstScan));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(secondBucket)), greaterThanOrEqualTo(beforeFirstScan));

        final Instant beforeSecondScan = Instant.now();
        final List<PartitionIdentifier> secondScanPartitions = partitionCreationSupplier.apply(globalStateMap);
        assertThat(secondScanPartitions.isEmpty(), equalTo(true));

        final List<List<PartitionIdentifier>> createdPartitions = createPartitionsArgumentCaptor.getAllValues();
        assertThat(createdPartitions.size(), equalTo(4));
        assertThat(createdPartitions.get(0).size(), equalTo(expectedPartitionIdentifiersFirstBucket.size()));
        assertThat(createdPartitions.get(0).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersFirstBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(createdPartitions.get(1).size(), equalTo(expectedPartitionIdentifiersSecondBucket.size()));
        assertThat(createdPartitions.get(1).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersSecondBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(createdPartitions.get(2).size(), equalTo(expectedPartitionIdentifiersSecondScanFirstBucket.size()));
        assertThat(createdPartitions.get(2).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersSecondScanFirstBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(createdPartitions.get(3).size(), equalTo(expectedPartitionIdentifiersSecondScanSecondBucket.size()));
        assertThat(createdPartitions.get(3).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersSecondScanSecondBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(globalStateMap, notNullValue());
        assertThat(globalStateMap.containsKey(SCAN_COUNT), equalTo(true));
        assertThat(globalStateMap.get(SCAN_COUNT), equalTo(2));
        assertThat(globalStateMap.containsKey(firstBucket), equalTo(true));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(firstBucket)), lessThanOrEqualTo(mostRecentSecondScan));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(firstBucket)), greaterThanOrEqualTo(beforeSecondScan));
        assertThat(globalStateMap.containsKey(secondBucket), equalTo(true));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(secondBucket)), lessThanOrEqualTo(mostRecentSecondScan));
        assertThat(Instant.parse((CharSequence) globalStateMap.get(secondBucket)), greaterThan(beforeSecondScan));
        assertThat(Instant.ofEpochMilli((Long) globalStateMap.get(LAST_SCAN_TIME)).isBefore(Instant.now()), equalTo(true));

        assertThat(partitionCreationSupplier.apply(globalStateMap), equalTo(Collections.emptyList()));

        verifyNoMoreInteractions(sourceCoordinator);

        verify(listObjectsResponse, times(4)).contents();
    }


    @Test
    void scheduled_scan_filters_on_start_time_and_end_time_for_the_first_scan_and_does_not_filter_on_subsequent_scans() {
        schedulingOptions = mock(S3ScanSchedulingOptions.class);
        given(schedulingOptions.getCount()).willReturn(2);

        final String firstScanBucket = "bucket-one";
        final String notFirstScanBucket = "bucket-two";

        final Map<String, Object> globalStateMap = new HashMap<>();
        globalStateMap.put(firstScanBucket, null);
        globalStateMap.put(notFirstScanBucket, "2024-09-07T20:43:34.384822Z");
        globalStateMap.put(SCAN_COUNT, 0);

        final LocalDateTime startTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(1725907846000L), ZoneId.systemDefault());
        final LocalDateTime endTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(1725907849100L), ZoneId.systemDefault());

        final ScanOptions firstBucketScanOptions = mock(ScanOptions.class);
        final S3ScanBucketOption firstBucketScanBucketOption = mock(S3ScanBucketOption.class);
        given(firstBucketScanOptions.getBucketOption()).willReturn(firstBucketScanBucketOption);
        given(firstBucketScanBucketOption.getName()).willReturn(firstScanBucket);
        given(firstBucketScanOptions.getUseStartDateTime()).willReturn(startTime);
        given(firstBucketScanOptions.getUseEndDateTime()).willReturn(endTime);
        scanOptionsList.add(firstBucketScanOptions);

        final ScanOptions notFirstScanOptions = mock(ScanOptions.class);
        final S3ScanBucketOption notFirstScanBucketOption = mock(S3ScanBucketOption.class);
        given(notFirstScanOptions.getBucketOption()).willReturn(notFirstScanBucketOption);
        given(notFirstScanBucketOption.getName()).willReturn(notFirstScanBucket);
        given(notFirstScanOptions.getUseStartDateTime()).willReturn(startTime);
        given(notFirstScanOptions.getUseEndDateTime()).willReturn(endTime);
        scanOptionsList.add(notFirstScanOptions);

        final ListObjectsV2Response listObjectsResponse = mock(ListObjectsV2Response.class);
        final List<S3Object> s3ObjectsList = new ArrayList<>();

        final Instant objectNotBetweenStartAndEndTime = Instant.ofEpochMilli(1725907846000L).minus(500L, TimeUnit.SECONDS.toChronoUnit());
        final S3Object validObject = mock(S3Object.class);
        given(validObject.key()).willReturn("valid");
        given(validObject.lastModified()).willReturn(objectNotBetweenStartAndEndTime);
        s3ObjectsList.add(validObject);

        final ArgumentCaptor<List<PartitionIdentifier>> createPartitionsArgumentCaptor = ArgumentCaptor.forClass(List.class);
        doNothing().when(sourceCoordinator).createPartitions(createPartitionsArgumentCaptor.capture());

        final List<PartitionIdentifier> expectedPartitionIdentifiers = new ArrayList<>();
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(notFirstScanBucket + "|" + validObject.key()).build());

        given(listObjectsResponse.contents())
                .willReturn(s3ObjectsList);

        given(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).willReturn(listObjectsResponse);

        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = createObjectUnderTest();

        final List<PartitionIdentifier> firstScanPartitions = partitionCreationSupplier.apply(globalStateMap);
        assertThat(firstScanPartitions.isEmpty(), equalTo(true));

        final List<List<PartitionIdentifier>> createdPartitions = createPartitionsArgumentCaptor.getAllValues();
        assertThat(createdPartitions.size(), equalTo(2));
        assertThat(createdPartitions.get(0).isEmpty(), equalTo(true));

        assertThat(createdPartitions.get(1).size(), equalTo(expectedPartitionIdentifiers.size()));
        assertThat(createdPartitions.get(1).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiers.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        final List<PartitionIdentifier> secondScanPartitions = partitionCreationSupplier.apply(globalStateMap);
        assertThat(secondScanPartitions.isEmpty(), equalTo(true));

        verifyNoMoreInteractions(sourceCoordinator);
    }

    @Test
    void getNextPartition_with_folder_partitioning_enabled_returns_the_expected_partition_identifiers() {
        folderPartitioningOptions = mock(FolderPartitioningOptions.class);
        when(folderPartitioningOptions.getFolderDepth()).thenReturn(1);

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

        final ArgumentCaptor<List<PartitionIdentifier>> createPartitionsArgumentCaptor = ArgumentCaptor.forClass(List.class);
        doNothing().when(sourceCoordinator).createPartitions(createPartitionsArgumentCaptor.capture());

        final List<PartitionIdentifier> expectedPartitionIdentifiersFirstBucket = new ArrayList<>();
        final List<PartitionIdentifier> expectedPartitionIdentifiersSecondBucket = new ArrayList<>();

        final ListObjectsV2Response listObjectsResponse = mock(ListObjectsV2Response.class);
        final List<S3Object> s3ObjectsList = new ArrayList<>();

        final S3Object invalidFolderObject = mock(S3Object.class);
        given(invalidFolderObject.key()).willReturn("folder-key/");
        given(invalidFolderObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(invalidFolderObject);

        final S3Object invalidForFirstBucketSuffixObject = mock(S3Object.class);
        given(invalidForFirstBucketSuffixObject.key()).willReturn("folder-1/test.invalid");
        given(invalidForFirstBucketSuffixObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(invalidForFirstBucketSuffixObject);
        expectedPartitionIdentifiersSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + "folder-1/").build());

        final S3Object invalidDueToLastModifiedOutsideOfStartEndObject = mock(S3Object.class);
        given(invalidDueToLastModifiedOutsideOfStartEndObject.key()).willReturn(UUID.randomUUID().toString());
        given(invalidDueToLastModifiedOutsideOfStartEndObject.lastModified()).willReturn(Instant.now().minus(3, ChronoUnit.MINUTES));
        s3ObjectsList.add(invalidDueToLastModifiedOutsideOfStartEndObject);

        final S3Object validObject = mock(S3Object.class);
        given(validObject.key()).willReturn("folder-1/valid");
        given(validObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(validObject);
        expectedPartitionIdentifiersFirstBucket.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + "folder-1/").build());

        final S3Object newFolderObject = mock(S3Object.class);
        given(newFolderObject.key()).willReturn("folder-2/valid");
        given(newFolderObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(newFolderObject);
        expectedPartitionIdentifiersSecondBucket.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + "folder-2/").build());
        expectedPartitionIdentifiersFirstBucket.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + "folder-2/").build());

        final S3Object noDepthFoundForFolder = mock(S3Object.class);
        given(noDepthFoundForFolder.key()).willReturn("no_folder.json");
        given(noDepthFoundForFolder.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(noDepthFoundForFolder);

        given(listObjectsResponse.contents()).willReturn(s3ObjectsList);

        final ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        given(s3Client.listObjectsV2(listObjectsV2RequestArgumentCaptor.capture())).willReturn(listObjectsResponse);

        final Map<String, Object> globalStateMap = new HashMap<>();
        final List<PartitionIdentifier> resultingPartitions = partitionCreationSupplier.apply(globalStateMap);
        assertThat(resultingPartitions.isEmpty(), equalTo(true));

        assertThat(globalStateMap, notNullValue());
        assertThat(globalStateMap.containsKey(SCAN_COUNT), equalTo(true));
        assertThat(globalStateMap.get(SCAN_COUNT), equalTo(1));

        globalStateMap.put(secondBucket, null);

        assertThat(partitionCreationSupplier.apply(globalStateMap), equalTo(Collections.emptyList()));


        final List<List<PartitionIdentifier>> createdPartitions = createPartitionsArgumentCaptor.getAllValues();
        assertThat(createdPartitions.size(), equalTo(2));
        assertThat(createdPartitions.get(0).size(), equalTo(expectedPartitionIdentifiersFirstBucket.size()));
        assertThat(createdPartitions.get(0).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersFirstBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        assertThat(createdPartitions.get(1).size(), equalTo(expectedPartitionIdentifiersSecondBucket.size()));
        assertThat(createdPartitions.get(1).stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiersSecondBucket.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));

        verifyNoMoreInteractions(sourceCoordinator);
    }
}
