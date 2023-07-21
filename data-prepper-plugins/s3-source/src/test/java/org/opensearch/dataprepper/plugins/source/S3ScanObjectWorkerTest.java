/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanSchedulingOptions;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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

    @Mock
    private S3SourceConfig s3SourceConfig;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private S3ScanScanOptions s3ScanScanOptions;

    @Mock
    private S3ObjectDeleteWorker s3ObjectDeleteWorker;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private DeleteObjectRequest deleteObjectRequest;

    @Mock
    private S3ScanSchedulingOptions s3ScanSchedulingOptions;

    @Mock
    private PluginMetrics pluginMetrics;

    private List<ScanOptions> scanOptionsList;

    @BeforeEach
    void setup() {
        scanOptionsList = new ArrayList<>();
    }

    private ScanObjectWorker createObjectUnderTest() {
        when(s3ScanScanOptions.getSchedulingOptions()).thenReturn(s3ScanSchedulingOptions);
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        final ScanObjectWorker objectUnderTest = new ScanObjectWorker(s3Client, scanOptionsList, s3ObjectHandler, bucketOwnerProvider,
                sourceCoordinator, s3SourceConfig, acknowledgementSetManager, s3ObjectDeleteWorker, pluginMetrics);
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

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doThrow(exception).when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).giveUpPartitions();

        createObjectUnderTest().runWithoutInfiniteLoop();

        verifyNoMoreInteractions(sourceCoordinator);
    }

    @Test
    void partition_from_getNextPartition_is_processed_correctly() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).closePartition(anyString(), any(), anyInt());

        createObjectUnderTest().runWithoutInfiniteLoop();

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void deleteS3Object_should_be_invoked_after_processing_when_deleteS3Objects_is_true() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(false);
        when(s3SourceConfig.isDeleteS3Objects()).thenReturn(true);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, objectKey)).thenReturn(deleteObjectRequest);
        when(s3ScanSchedulingOptions.getJobCount()).thenReturn(1);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(acknowledgementSet), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).closePartition(anyString(), any(), anyInt());
        when(acknowledgementSetManager.create(any(), any())).thenReturn(acknowledgementSet);

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        scanObjectWorker.runWithoutInfiniteLoop();

        verify(acknowledgementSetManager).create(any(), any());
        verify(sourceCoordinator).closePartition(partitionKey, s3ScanSchedulingOptions.getRate(), s3ScanSchedulingOptions.getJobCount());
        verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, objectKey);
        verify(s3ObjectDeleteWorker).deleteS3Object(deleteObjectRequest);

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void deleteS3Object_should_not_be_invoked_after_processing_when_deleteS3Objects_is_false() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(false);
        when(s3SourceConfig.isDeleteS3Objects()).thenReturn(false);
        when(s3ScanSchedulingOptions.getJobCount()).thenReturn(1);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(acknowledgementSet), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).closePartition(anyString(), any(), anyInt());
        when(acknowledgementSetManager.create(any(), any())).thenReturn(acknowledgementSet);

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        scanObjectWorker.runWithoutInfiniteLoop();

        verify(acknowledgementSetManager).create(any(), any());
        verify(sourceCoordinator).closePartition(partitionKey, s3ScanSchedulingOptions.getRate(), s3ScanSchedulingOptions.getJobCount());
        verifyNoInteractions(s3ObjectDeleteWorker);

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void deleteS3Object_should_be_invoked_after_closed_count_greater_than_or_equal_to_job_count() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;

        when(s3SourceConfig.isDeleteS3Objects()).thenReturn(true);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, objectKey)).thenReturn(deleteObjectRequest);
        when(s3ScanSchedulingOptions.getJobCount()).thenReturn(2);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(acknowledgementSet), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).closePartition(anyString(), any(), anyInt());
        when(acknowledgementSetManager.create(any(), any())).thenReturn(acknowledgementSet);

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        scanObjectWorker.runWithoutInfiniteLoop();

        verify(acknowledgementSetManager).create(any(), any());
        verify(sourceCoordinator).closePartition(partitionKey, s3ScanSchedulingOptions.getRate(), s3ScanSchedulingOptions.getJobCount());
        // no interactions when closed count < job count
        verifyNoInteractions(s3ObjectDeleteWorker);

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));

        final SourcePartition<S3SourceProgressState> processedPartition = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(1L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(processedPartition));
        scanObjectWorker.runWithoutInfiniteLoop();

        verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, objectKey);
        verify(s3ObjectDeleteWorker).deleteS3Object(deleteObjectRequest);
    }

    @Test
    void getNextPartition_supplier_is_expected_partitionCreationSupplier() {
        given(sourceCoordinator.getNextPartition(any(S3ScanPartitionCreationSupplier.class))).willReturn(Optional.empty());
        final ScanObjectWorker objectUnderTest = createObjectUnderTest();
        objectUnderTest.runWithoutInfiniteLoop();
    }

    static Stream<Class> exceptionProvider() {
        return Stream.of(PartitionUpdateException.class, PartitionNotFoundException.class, PartitionNotOwnedException.class);
    }
}
