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
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
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
    void getNextPartition_supplier_is_expected_partitionCreationSupplier() {
        given(sourceCoordinator.getNextPartition(any(S3ScanPartitionCreationSupplier.class))).willReturn(Optional.empty());
        final ScanObjectWorker objectUnderTest = createObjectUnderTest();
        objectUnderTest.runWithoutInfiniteLoop();
    }

    static Stream<Class> exceptionProvider() {
        return Stream.of(PartitionUpdateException.class, PartitionNotFoundException.class, PartitionNotOwnedException.class);
    }
}
