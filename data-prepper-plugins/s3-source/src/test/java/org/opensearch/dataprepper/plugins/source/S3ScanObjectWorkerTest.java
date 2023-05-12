/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;


import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class S3ScanObjectWorkerTest {

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    @Mock
    private S3Client s3Client;

    @Mock
    private S3ObjectHandler s3ObjectHandler;

    @Mock
    private SourceCoordinator<S3SourceProgressState> sourceCoordinator;

    private List<ScanOptions> scanOptionsList;

    @BeforeEach
    void setup() {
        scanOptionsList = new ArrayList<>();
    }

    private ScanObjectWorker createObjectUnderTest() {
        final ScanObjectWorker objectUnderTest = new ScanObjectWorker(s3Client, scanOptionsList, s3ObjectHandler, bucketOwnerProvider, sourceCoordinator);
        verify(sourceCoordinator).initialize();
        return objectUnderTest;
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void giveUpPartitions_is_called_when_a_PartitionException_is_thrown_from_parseS3Object(final Class exception) throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class).withPartitionKey(partitionKey).build();

        given(sourceCoordinator.getNextPartition(any(Supplier.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doThrow(exception).when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(null));
        doNothing().when(sourceCoordinator).giveUpPartitions();

        createObjectUnderTest().runWithoutInfiniteLoop();

        verifyNoMoreInteractions(sourceCoordinator);
    }

    @Test
    void partition_from_getNextPartition_is_processed_correctly() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class).withPartitionKey(partitionKey).build();

        given(sourceCoordinator.getNextPartition(any(Supplier.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(null));
        doNothing().when(sourceCoordinator).completePartition(partitionKey);

        createObjectUnderTest().runWithoutInfiniteLoop();

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void getNextPartition_supplier_returns_expected_PartitionIdentifiers() {
        final ArgumentCaptor<Supplier<List<PartitionIdentifier>>> partitionSupplierArgumentCaptor = ArgumentCaptor.forClass(Supplier.class);

        given(sourceCoordinator.getNextPartition(partitionSupplierArgumentCaptor.capture())).willReturn(Optional.empty());

        final String firstBucket = UUID.randomUUID().toString();
        final String secondBucket = UUID.randomUUID().toString();

        final Instant startTime = Instant.now();
        final Instant endTime = Instant.now().plus(3, ChronoUnit.MINUTES);


        final ScanOptions firstBucketScanOptions = mock(ScanOptions.class);
        given(firstBucketScanOptions.getBucket()).willReturn(firstBucket);
        given(firstBucketScanOptions.getUseStartDateTime()).willReturn(LocalDateTime.ofInstant(startTime, ZoneId.systemDefault()));
        given(firstBucketScanOptions.getUseEndDateTime()).willReturn(LocalDateTime.ofInstant(endTime, ZoneId.systemDefault()));
        final S3ScanKeyPathOption firstBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(firstBucketScanOptions.getS3ScanKeyPathOption()).willReturn(firstBucketScanKeyPath);
        given(firstBucketScanKeyPath.getS3scanIncludeOptions()).willReturn(List.of(UUID.randomUUID().toString()));
        given(firstBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(List.of(".invalid"));
        scanOptionsList.add(firstBucketScanOptions);

        final ScanOptions secondBucketScanOptions = mock(ScanOptions.class);
        given(secondBucketScanOptions.getBucket()).willReturn(secondBucket);
        given(secondBucketScanOptions.getUseStartDateTime()).willReturn(LocalDateTime.ofInstant(startTime, ZoneId.systemDefault()));
        given(secondBucketScanOptions.getUseEndDateTime()).willReturn(LocalDateTime.ofInstant(endTime, ZoneId.systemDefault()));
        final S3ScanKeyPathOption secondBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(secondBucketScanOptions.getS3ScanKeyPathOption()).willReturn(secondBucketScanKeyPath);
        given(secondBucketScanKeyPath.getS3scanIncludeOptions()).willReturn(null);
        given(secondBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(null);
        scanOptionsList.add(secondBucketScanOptions);


        final ScanObjectWorker objectUnderTest = createObjectUnderTest();
        objectUnderTest.runWithoutInfiniteLoop();

        final Supplier<List<PartitionIdentifier>> partitionCreationSupplier = partitionSupplierArgumentCaptor.getValue();

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

        final List<PartitionIdentifier> resultingPartitions = partitionCreationSupplier.get();

        assertThat(resultingPartitions, notNullValue());
        assertThat(resultingPartitions.size(), equalTo(expectedPartitionIdentifiers.size()));
        assertThat(resultingPartitions.stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiers.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));
    }

    static Stream<Class> exceptionProvider() {
        return Stream.of(PartitionUpdateException.class, PartitionNotFoundException.class, PartitionNotOwnedException.class);
    }
}
