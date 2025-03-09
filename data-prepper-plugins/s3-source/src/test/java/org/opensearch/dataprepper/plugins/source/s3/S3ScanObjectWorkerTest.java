/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;


import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.ProgressCheck;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.s3.configuration.FolderPartitioningOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanSchedulingOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.model.source.s3.S3ScanEnvironmentVariables.STOP_S3_SCAN_PROCESSING_PROPERTY;
import static org.opensearch.dataprepper.plugins.source.s3.ScanObjectWorker.ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.s3.ScanObjectWorker.CHECKPOINT_OWNERSHIP_INTERVAL;
import static org.opensearch.dataprepper.plugins.source.s3.ScanObjectWorker.NO_OBJECTS_FOUND_BEFORE_PARTITION_DELETION_DURATION;
import static org.opensearch.dataprepper.plugins.source.s3.ScanObjectWorker.NO_OBJECTS_FOUND_FOR_FOLDER_PARTITION;
import static org.opensearch.dataprepper.plugins.source.s3.ScanObjectWorker.PARTITION_OWNERSHIP_UPDATE_ERRORS;

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

    @Mock
    private Counter counter;

    @Mock
    private Counter partitionOwnershipUpdateErrorCounter;

    @Mock
    private Counter noObjectsFoundForFolderPartitionCounter;

    private List<ScanOptions> scanOptionsList;

    @Mock
    private Duration acknowledgmentSetTimeout;
    @Mock
    S3ScanBucketOption s3ScanBucketOption;
    @Mock
    S3ScanBucketOptions s3ScanBucketOptions;
    String bucket;

    @BeforeEach
    void setup() {
        scanOptionsList = new ArrayList<>();
        when(s3ScanScanOptions.getPartitioningOptions()).thenReturn(null);
        when(s3ScanScanOptions.getAcknowledgmentTimeout()).thenReturn(acknowledgmentSetTimeout);
        s3ScanBucketOption = mock(S3ScanBucketOption.class);
        s3ScanBucketOptions = mock(S3ScanBucketOptions.class);
        bucket = UUID.randomUUID().toString();
        when(s3ScanBucketOption.getName()).thenReturn(bucket);
        when(s3ScanBucketOption.getDataSelection()).thenReturn(S3DataSelection.DATA_AND_METADATA);
        when(s3ScanBucketOptions.getS3ScanBucketOption()).thenReturn(s3ScanBucketOption);
        when(s3ScanScanOptions.getBuckets()).thenReturn(List.of(s3ScanBucketOptions));
    }

    private ScanObjectWorker createObjectUnderTest() {
        when(s3ScanScanOptions.getSchedulingOptions()).thenReturn(s3ScanSchedulingOptions);
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        when(pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(NO_OBJECTS_FOUND_FOR_FOLDER_PARTITION)).thenReturn(noObjectsFoundForFolderPartitionCounter);
        when(pluginMetrics.counter(PARTITION_OWNERSHIP_UPDATE_ERRORS)).thenReturn(partitionOwnershipUpdateErrorCounter);
        final ScanObjectWorker objectUnderTest = new ScanObjectWorker(s3Client, scanOptionsList, s3ObjectHandler, bucketOwnerProvider,
                sourceCoordinator, s3SourceConfig, acknowledgementSetManager, s3ObjectDeleteWorker, 30000, pluginMetrics);
        verify(sourceCoordinator).initialize();
        return objectUnderTest;
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void giveUpPartitions_is_called_when_a_PartitionException_is_thrown_from_processS3Object(final Class exception) throws IOException {
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class).withPartitionKey(partitionKey).build();

        given(sourceCoordinator.getNextPartition(any(Function.class), eq(false))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doThrow(exception).when(s3ObjectHandler).processS3Object(objectReferenceArgumentCaptor.capture(), eq(S3DataSelection.DATA_AND_METADATA), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).giveUpPartition(any());

        createObjectUnderTest().runWithoutInfiniteLoop();

        verifyNoMoreInteractions(sourceCoordinator);
    }

    @Test
    void partition_from_getNextPartition_is_processed_correctly() throws IOException {
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class), eq(false))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).processS3Object(objectReferenceArgumentCaptor.capture(), eq(S3DataSelection.DATA_AND_METADATA), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString(), eq(false));

        createObjectUnderTest().runWithoutInfiniteLoop();

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void buildDeleteObjectRequest_should_be_invoked_after_processing_when_deleteS3Objects_and_acknowledgements_is_true() throws IOException {
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(true);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(true);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, objectKey)).thenReturn(deleteObjectRequest);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class), eq(false))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).processS3Object(objectReferenceArgumentCaptor.capture(), eq(S3DataSelection.DATA_AND_METADATA), eq(acknowledgementSet), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString(), eq(true));

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class))).thenReturn(acknowledgementSet);
        doNothing().when(acknowledgementSet).addProgressCheck(any(Consumer.class), any(Duration.class));

        scanObjectWorker.runWithoutInfiniteLoop();

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager).create(consumerArgumentCaptor.capture(), any(Duration.class));

        final ArgumentCaptor<Consumer> progressCheckArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSet).addProgressCheck(progressCheckArgumentCaptor.capture(), eq(CHECKPOINT_OWNERSHIP_INTERVAL));

        final Consumer<ProgressCheck> progressCheckConsumer = progressCheckArgumentCaptor.getValue();
        progressCheckConsumer.accept(mock(ProgressCheck.class));

        final Consumer<Boolean> ackCallback = consumerArgumentCaptor.getValue();
        ackCallback.accept(true);

        final InOrder inOrder = inOrder(sourceCoordinator, acknowledgementSet, s3ObjectDeleteWorker);
        inOrder.verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, objectKey);
        inOrder.verify(sourceCoordinator).updatePartitionForAcknowledgmentWait(partitionKey, acknowledgmentSetTimeout);
        inOrder.verify(acknowledgementSet).complete();
        inOrder.verify(sourceCoordinator).renewPartitionOwnership(partitionKey);
        inOrder.verify(sourceCoordinator).completePartition(partitionKey, true);

        verify(counter).increment();

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void acknowledgment_progress_check_increments_ownership_error_metric_when_partition_fails_to_update(final Class<Throwable> exception) throws IOException {
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(true);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(true);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, objectKey)).thenReturn(deleteObjectRequest);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class), eq(false))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).processS3Object(objectReferenceArgumentCaptor.capture(), eq(S3DataSelection.DATA_AND_METADATA), eq(acknowledgementSet), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString(), eq(true));

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class))).thenReturn(acknowledgementSet);
        doNothing().when(acknowledgementSet).addProgressCheck(any(Consumer.class), any(Duration.class));

        scanObjectWorker.runWithoutInfiniteLoop();

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager).create(consumerArgumentCaptor.capture(), any(Duration.class));

        final ArgumentCaptor<Consumer> progressCheckArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSet).addProgressCheck(progressCheckArgumentCaptor.capture(), eq(CHECKPOINT_OWNERSHIP_INTERVAL));

        final Consumer<ProgressCheck> progressCheckConsumer = progressCheckArgumentCaptor.getValue();
        doThrow(exception).when(sourceCoordinator).renewPartitionOwnership(partitionKey);
        progressCheckConsumer.accept(mock(ProgressCheck.class));

        final Consumer<Boolean> ackCallback = consumerArgumentCaptor.getValue();
        ackCallback.accept(true);

        final InOrder inOrder = inOrder(sourceCoordinator, acknowledgementSet, s3ObjectDeleteWorker);
        inOrder.verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, objectKey);
        inOrder.verify(sourceCoordinator).updatePartitionForAcknowledgmentWait(partitionKey, acknowledgmentSetTimeout);
        inOrder.verify(acknowledgementSet).complete();
        inOrder.verify(sourceCoordinator).renewPartitionOwnership(partitionKey);
        inOrder.verify(sourceCoordinator).completePartition(partitionKey, true);

        verify(counter).increment();
        verify(partitionOwnershipUpdateErrorCounter).increment();

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void buildDeleteObjectRequest_should_not_be_invoked_after_processing_when_deleteS3Objects_is_true_acknowledgements_is_false() throws IOException {
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(false);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(true);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class), eq(false))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).processS3Object(objectReferenceArgumentCaptor.capture(), eq(S3DataSelection.DATA_AND_METADATA), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString(), eq(false));

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        scanObjectWorker.runWithoutInfiniteLoop();

        verify(sourceCoordinator).completePartition(partitionKey, false);
        verify(sourceCoordinator, times(0)).updatePartitionForAcknowledgmentWait(anyString(), any(Duration.class));
        verifyNoInteractions(s3ObjectDeleteWorker);
        verifyNoInteractions(acknowledgementSetManager);

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void deleteS3Object_should_not_be_invoked_after_processing_when_deleteS3Objects_is_false() throws IOException {
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(false);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(false);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class), eq(false))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).processS3Object(objectReferenceArgumentCaptor.capture(), eq(S3DataSelection.DATA_AND_METADATA), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString(), eq(false));

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        scanObjectWorker.runWithoutInfiniteLoop();

        verifyNoInteractions(acknowledgementSetManager);
        verify(sourceCoordinator).completePartition(partitionKey, false);
        verifyNoInteractions(s3ObjectDeleteWorker);

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void getNextPartition_supplier_is_expected_partitionCreationSupplier() {
        given(sourceCoordinator.getNextPartition(any(S3ScanPartitionCreationSupplier.class), eq(false))).willReturn(Optional.empty());
        final ScanObjectWorker objectUnderTest = createObjectUnderTest();
        objectUnderTest.runWithoutInfiniteLoop();
    }

    @Test
    void partitionIsCompleted_when_NoObjectKeyException_is_thrown_from_process_object() throws IOException {
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class).withPartitionKey(partitionKey).build();

        given(sourceCoordinator.getNextPartition(any(Function.class), eq(false))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doThrow(NoSuchKeyException.class).when(s3ObjectHandler).processS3Object(objectReferenceArgumentCaptor.capture(), eq(S3DataSelection.DATA_AND_METADATA), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(partitionKey, false);

        createObjectUnderTest().runWithoutInfiniteLoop();

        verifyNoMoreInteractions(sourceCoordinator);
    }

    @Test
    void processing_with_folder_partitions_with_no_objects_gives_up_that_partition() {

        final FolderPartitioningOptions folderPartitioningOptions = mock(FolderPartitioningOptions.class);
        when(s3ScanScanOptions.getPartitioningOptions()).thenReturn(folderPartitioningOptions);

        final String folder = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + folder;

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class).withPartitionKey(partitionKey).build();
        when(sourceCoordinator.getNextPartition(any(Function.class), eq(true))).thenReturn(Optional.of(partitionToProcess));

        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        when(listObjectsV2Response.isTruncated()).thenReturn(false);
        when(listObjectsV2Response.contents()).thenReturn(Collections.emptyList());

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);
        doNothing().when(sourceCoordinator).giveUpPartition(eq(partitionKey), any(Instant.class));
        doNothing().when(sourceCoordinator).saveProgressStateForPartition(eq(partitionKey), any(S3SourceProgressState.class));

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();
        scanObjectWorker.runWithoutInfiniteLoop();

        verify(sourceCoordinator).saveProgressStateForPartition(eq(partitionKey), any(S3SourceProgressState.class));
        verify(sourceCoordinator).giveUpPartition(eq(partitionKey), any(Instant.class));

        final ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);

        verify(s3Client).listObjectsV2(listObjectsV2RequestArgumentCaptor.capture());

        final ListObjectsV2Request request = listObjectsV2RequestArgumentCaptor.getValue();
        assertThat(request, notNullValue());
        assertThat(request.bucket(), equalTo(bucket));
        assertThat(request.fetchOwner(), equalTo(true));
        assertThat(request.prefix(), equalTo(folder));
    }

    @Test
    void processing_with_folder_partition_with_no_objects_found_for_some_time_deletes_the_partition() {
        final FolderPartitioningOptions folderPartitioningOptions = mock(FolderPartitioningOptions.class);
        when(s3ScanScanOptions.getPartitioningOptions()).thenReturn(folderPartitioningOptions);

        final String folder = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + folder;

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition
                .builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionState(new S3SourceProgressState(Instant.now()
                        .minus(NO_OBJECTS_FOUND_BEFORE_PARTITION_DELETION_DURATION)
                        .minus(Duration.ofMinutes(1)).toEpochMilli()))
                .build();
        when(sourceCoordinator.getNextPartition(any(Function.class), eq(true))).thenReturn(Optional.of(partitionToProcess));

        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        when(listObjectsV2Response.isTruncated()).thenReturn(false);
        when(listObjectsV2Response.contents()).thenReturn(Collections.emptyList());

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);

        doNothing().when(sourceCoordinator).deletePartition(partitionKey);

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();
        scanObjectWorker.runWithoutInfiniteLoop();
        verify(sourceCoordinator).deletePartition(partitionKey);
        verify(noObjectsFoundForFolderPartitionCounter).increment();
    }

    @Test
    void processing_with_folder_partition_processes_objects_in_folder_and_deletes_them_on_callback() throws IOException {
        when(s3SourceConfig.getAcknowledgements()).thenReturn(true);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(true);

        final FolderPartitioningOptions folderPartitioningOptions = mock(FolderPartitioningOptions.class);
        when(folderPartitioningOptions.getMaxObjectsPerOwnership()).thenReturn(3);
        when(s3ScanScanOptions.getPartitioningOptions()).thenReturn(folderPartitioningOptions);

        final String folder = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + folder;

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition
                .builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionState(new S3SourceProgressState(Instant.now()
                        .minus(NO_OBJECTS_FOUND_BEFORE_PARTITION_DELETION_DURATION)
                        .minus(Duration.ofMinutes(1)).toEpochMilli()))
                .build();
        when(sourceCoordinator.getNextPartition(any(Function.class), eq(true))).thenReturn(Optional.of(partitionToProcess));
        doNothing().when(sourceCoordinator).saveProgressStateForPartition(eq(partitionKey), any(S3SourceProgressState.class));

        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        when(listObjectsV2Response.isTruncated()).thenReturn(false);

        final S3Object firstObject = mock(S3Object.class);
        when(firstObject.key()).thenReturn(UUID.randomUUID().toString());
        final DeleteObjectRequest firstObjectDeleteRequest = mock(DeleteObjectRequest.class);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, firstObject.key())).thenReturn(firstObjectDeleteRequest);

        final S3Object secondObject = mock(S3Object.class);
        when(secondObject.key()).thenReturn(UUID.randomUUID().toString());
        final DeleteObjectRequest secondObjectDeleteRequest = mock(DeleteObjectRequest.class);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, secondObject.key())).thenReturn(secondObjectDeleteRequest);

        when(listObjectsV2Response.contents()).thenReturn(List.of(firstObject, secondObject));
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);

        final AcknowledgementSet acknowledgementSet1 = mock(AcknowledgementSet.class);
        final AcknowledgementSet acknowledgementSet2 = mock(AcknowledgementSet.class);

        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenReturn(acknowledgementSet1)
                .thenReturn(acknowledgementSet2);

        doNothing().when(acknowledgementSet1).addProgressCheck(any(Consumer.class), eq(CHECKPOINT_OWNERSHIP_INTERVAL));
        doNothing().when(acknowledgementSet2).addProgressCheck(any(Consumer.class), eq(CHECKPOINT_OWNERSHIP_INTERVAL));

        doNothing().when(s3ObjectDeleteWorker).deleteS3Object(any(DeleteObjectRequest.class));
        doNothing().when(s3ObjectHandler).processS3Object(any(S3ObjectReference.class), any(S3DataSelection.class), any(AcknowledgementSet.class), eq(sourceCoordinator), eq(partitionKey));

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();
        scanObjectWorker.runWithoutInfiniteLoop();

        verify(sourceCoordinator).saveProgressStateForPartition(eq(partitionKey), any(S3SourceProgressState.class));

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager, times(2)).create(consumerArgumentCaptor.capture(), any(Duration.class));

        final List<Consumer> ackCallbacks = consumerArgumentCaptor.getAllValues();
        assertThat(ackCallbacks.size(), equalTo(2));

        final InOrder inOrder = inOrder(sourceCoordinator, acknowledgementSet2, acknowledgementSet1, s3ObjectDeleteWorker);

        inOrder.verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, firstObject.key());
        inOrder.verify(acknowledgementSet1).complete();
        inOrder.verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, secondObject.key());
        inOrder.verify(sourceCoordinator).updatePartitionForAcknowledgmentWait(partitionKey, acknowledgmentSetTimeout);
        inOrder.verify(acknowledgementSet2).complete();

        final Consumer<Boolean> firstAckCallback = ackCallbacks.get(0);
        firstAckCallback.accept(true);

        inOrder.verify(s3ObjectDeleteWorker).deleteS3Object(firstObjectDeleteRequest);

        final Consumer<Boolean> secondAckCallback = ackCallbacks.get(1);
        secondAckCallback.accept(true);

        inOrder.verify(s3ObjectDeleteWorker).deleteS3Object(secondObjectDeleteRequest);
        inOrder.verify(sourceCoordinator).giveUpPartition(eq(partitionKey), any(Instant.class));
    }

    @Test
    void processing_with_folder_partition_processes_objects_in_folder_until_max_objects_per_ownership_is_reached() throws IOException {
        when(s3SourceConfig.getAcknowledgements()).thenReturn(true);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(true);

        final FolderPartitioningOptions folderPartitioningOptions = mock(FolderPartitioningOptions.class);
        when(folderPartitioningOptions.getMaxObjectsPerOwnership()).thenReturn(1);
        when(s3ScanScanOptions.getPartitioningOptions()).thenReturn(folderPartitioningOptions);

        final String folder = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + folder;

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition
                .builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionState(new S3SourceProgressState(Instant.now()
                        .minus(NO_OBJECTS_FOUND_BEFORE_PARTITION_DELETION_DURATION)
                        .minus(Duration.ofMinutes(1)).toEpochMilli()))
                .build();
        when(sourceCoordinator.getNextPartition(any(Function.class), eq(true))).thenReturn(Optional.of(partitionToProcess));
        doNothing().when(sourceCoordinator).saveProgressStateForPartition(eq(partitionKey), any(S3SourceProgressState.class));

        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        when(listObjectsV2Response.isTruncated()).thenReturn(false);

        final S3Object firstObject = mock(S3Object.class);
        when(firstObject.key()).thenReturn(UUID.randomUUID().toString());
        final DeleteObjectRequest firstObjectDeleteRequest = mock(DeleteObjectRequest.class);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, firstObject.key())).thenReturn(firstObjectDeleteRequest);

        final S3Object secondObject = mock(S3Object.class);
        when(secondObject.key()).thenReturn(UUID.randomUUID().toString());

        when(listObjectsV2Response.contents()).thenReturn(List.of(firstObject, secondObject));
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);

        final AcknowledgementSet acknowledgementSet1 = mock(AcknowledgementSet.class);

        when(acknowledgementSetManager.create(any(Consumer.class), any(Duration.class)))
                .thenReturn(acknowledgementSet1);
        doNothing().when(acknowledgementSet1).addProgressCheck(any(Consumer.class), eq(CHECKPOINT_OWNERSHIP_INTERVAL));

        doNothing().when(s3ObjectDeleteWorker).deleteS3Object(any(DeleteObjectRequest.class));
        doNothing().when(s3ObjectHandler).processS3Object(any(S3ObjectReference.class), any(S3DataSelection.class), any(AcknowledgementSet.class), eq(sourceCoordinator), eq(partitionKey));

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();
        scanObjectWorker.runWithoutInfiniteLoop();

        verify(sourceCoordinator).saveProgressStateForPartition(eq(partitionKey), any(S3SourceProgressState.class));

        final ArgumentCaptor<Consumer> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSetManager, times(1)).create(consumerArgumentCaptor.capture(), any(Duration.class));

        final ArgumentCaptor<Consumer> progressCheckArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSet1).addProgressCheck(progressCheckArgumentCaptor.capture(), eq(CHECKPOINT_OWNERSHIP_INTERVAL));

        final Consumer<ProgressCheck> progressCheckConsumer = progressCheckArgumentCaptor.getValue();
        progressCheckConsumer.accept(mock(ProgressCheck.class));
        verify(sourceCoordinator).renewPartitionOwnership(partitionKey);


        final InOrder inOrder = inOrder(sourceCoordinator, acknowledgementSet1, s3ObjectDeleteWorker);

        inOrder.verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, firstObject.key());
        inOrder.verify(sourceCoordinator).updatePartitionForAcknowledgmentWait(partitionKey, acknowledgmentSetTimeout);
        inOrder.verify(acknowledgementSet1).complete();

        final Consumer<Boolean> ackCallback = consumerArgumentCaptor.getValue();
        ackCallback.accept(true);

        inOrder.verify(s3ObjectDeleteWorker).deleteS3Object(firstObjectDeleteRequest);
        inOrder.verify(sourceCoordinator).giveUpPartition(eq(partitionKey), any(Instant.class));
    }

    @Test
    void running_with_system_property_to_stop_processing_does_not_process_objects() throws InterruptedException {
        System.setProperty(STOP_S3_SCAN_PROCESSING_PROPERTY, UUID.randomUUID().toString());

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.submit(scanObjectWorker::run);
        Thread.sleep(50);
        executorService.shutdownNow();

        verify(sourceCoordinator, never()).getNextPartition(any(), anyBoolean());

        System.clearProperty(STOP_S3_SCAN_PROCESSING_PROPERTY);

    }


    static Stream<Class> exceptionProvider() {
        return Stream.of(PartitionUpdateException.class, PartitionNotFoundException.class, PartitionNotOwnedException.class);
    }
}
