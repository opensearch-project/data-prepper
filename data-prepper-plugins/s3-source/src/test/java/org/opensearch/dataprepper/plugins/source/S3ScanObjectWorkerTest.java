/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;


import io.micrometer.core.instrument.Counter;
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
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.ScanObjectWorker.ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME;

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

    private List<ScanOptions> scanOptionsList;

    @BeforeEach
    void setup() {
        scanOptionsList = new ArrayList<>();
    }

    private ScanObjectWorker createObjectUnderTest() {
        when(s3ScanScanOptions.getSchedulingOptions()).thenReturn(s3ScanSchedulingOptions);
        when(s3SourceConfig.getS3ScanScanOptions()).thenReturn(s3ScanScanOptions);
        when(pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME)).thenReturn(counter);
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
        doNothing().when(sourceCoordinator).completePartition(anyString());

        createObjectUnderTest().runWithoutInfiniteLoop();

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void buildDeleteObjectRequest_should_be_invoked_after_processing_when_deleteS3Objects_and_acknowledgements_is_true() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(true);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(true);
        when(s3ObjectDeleteWorker.buildDeleteObjectRequest(bucket, objectKey)).thenReturn(deleteObjectRequest);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(acknowledgementSet), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString());

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        doAnswer(invocation -> {
            Consumer<Boolean> consumer = invocation.getArgument(0);
            consumer.accept(true);
            return acknowledgementSet;
        }).when(acknowledgementSetManager).create(any(Consumer.class), any(Duration.class));

        scanObjectWorker.runWithoutInfiniteLoop();

        verify(sourceCoordinator).completePartition(partitionKey);
        verify(s3ObjectDeleteWorker).buildDeleteObjectRequest(bucket, objectKey);
        verify(acknowledgementSet).complete();
        verify(counter).increment();

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void buildDeleteObjectRequest_should_not_be_invoked_after_processing_when_deleteS3Objects_is_true_acknowledgements_is_false() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        when(s3SourceConfig.getAcknowledgements()).thenReturn(false);
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(true);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString());

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        scanObjectWorker.runWithoutInfiniteLoop();

        verify(sourceCoordinator).completePartition(partitionKey);
        verifyNoInteractions(s3ObjectDeleteWorker);
        verifyNoInteractions(acknowledgementSetManager);

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
        when(s3SourceConfig.isDeleteS3ObjectsOnRead()).thenReturn(false);

        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doNothing().when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(anyString());

        final ScanObjectWorker scanObjectWorker = createObjectUnderTest();

        scanObjectWorker.runWithoutInfiniteLoop();

        verifyNoInteractions(acknowledgementSetManager);
        verify(sourceCoordinator).completePartition(partitionKey);
        verifyNoInteractions(s3ObjectDeleteWorker);

        final S3ObjectReference processedObject = objectReferenceArgumentCaptor.getValue();
        assertThat(processedObject.getBucketName(), equalTo(bucket));
        assertThat(processedObject.getKey(), equalTo(objectKey));
    }

    @Test
    void getNextPartition_supplier_is_expected_partitionCreationSupplier() {
        given(sourceCoordinator.getNextPartition(any(S3ScanPartitionCreationSupplier.class))).willReturn(Optional.empty());
        final ScanObjectWorker objectUnderTest = createObjectUnderTest();
        objectUnderTest.runWithoutInfiniteLoop();
    }

    @Test
    void partitionIsCompleted_when_NoObjectKeyException_is_thrown_from_process_object() throws IOException {
        final String bucket = UUID.randomUUID().toString();
        final String objectKey = UUID.randomUUID().toString();
        final String partitionKey = bucket + "|" + objectKey;


        final SourcePartition<S3SourceProgressState> partitionToProcess = SourcePartition.builder(S3SourceProgressState.class).withPartitionKey(partitionKey).build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));

        final ArgumentCaptor<S3ObjectReference> objectReferenceArgumentCaptor = ArgumentCaptor.forClass(S3ObjectReference.class);
        doThrow(NoSuchKeyException.class).when(s3ObjectHandler).parseS3Object(objectReferenceArgumentCaptor.capture(), eq(null), eq(sourceCoordinator), eq(partitionKey));
        doNothing().when(sourceCoordinator).completePartition(partitionKey);

        createObjectUnderTest().runWithoutInfiniteLoop();

        verifyNoMoreInteractions(sourceCoordinator);
    }

    static Stream<Class> exceptionProvider() {
        return Stream.of(PartitionUpdateException.class, PartitionNotFoundException.class, PartitionNotOwnedException.class);
    }
}
