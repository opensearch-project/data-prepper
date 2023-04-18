/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JavaClientAccumulatingBulkRequestTest {

    private BulkRequest.Builder bulkRequestBuilder;

    @BeforeEach
    void setUp() {
        bulkRequestBuilder = mock(BulkRequest.Builder.class);

        when(bulkRequestBuilder.operations(any(BulkOperation.class)))
                .thenReturn(bulkRequestBuilder);

    }

    private JavaClientAccumulatingBulkRequest createObjectUnderTest() {
        return new JavaClientAccumulatingBulkRequest(bulkRequestBuilder);
    }

    @Test
    void getOperationCount_returns_0_if_no_interactions() {
        assertThat(createObjectUnderTest().getOperationsCount(), equalTo(0));
    }

    @Test
    void getOperations_returns_empty_list_if_no_interactions() {
        assertThat(createObjectUnderTest().getOperations(),
                equalTo(Collections.emptyList()));
    }

    @Test
    void getOperations_returns_unmodifiable_list() {
        final List<BulkOperationWrapper> operations = createObjectUnderTest().getOperations();

        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(generateDocument()));
        assertThrows(UnsupportedOperationException.class, () -> operations.add(bulkOperation));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10})
    void getOperationsCount_returns_the_correct_operation_count(final int operationCount) {
        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();
        for (int i = 0; i < operationCount; i++) {
            final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(generateDocument()));
            objectUnderTest.addOperation(bulkOperation);
        }

        assertThat(objectUnderTest.getOperationsCount(), equalTo(operationCount));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10})
    void getEstimatedSizeInBytes_returns_the_current_size(final int operationCount) {
        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();
        final long arbitraryDocumentSize = 175;
        for (int i = 0; i < operationCount; i++) {
            final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(generateDocumentWithLength(arbitraryDocumentSize)));
            objectUnderTest.addOperation(bulkOperation);
        }

        final long expectedSize = operationCount * (arbitraryDocumentSize + JavaClientAccumulatingBulkRequest.OPERATION_OVERHEAD);
        assertThat(objectUnderTest.getEstimatedSizeInBytes(), equalTo(expectedSize));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10})
    void getEstimatedSizeInBytes_returns_the_operation_overhead_if_requests_have_no_documents(final int operationCount) {
        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();
        for (int i = 0; i < operationCount; i++) {
            objectUnderTest.addOperation(new BulkOperationWrapper(createBulkOperation(null)));
        }

        final long expectedSize = operationCount * JavaClientAccumulatingBulkRequest.OPERATION_OVERHEAD;
        assertThat(objectUnderTest.getEstimatedSizeInBytes(), equalTo(expectedSize));
    }

    @Test
    void getOperationAt_returns_the_correct_index() {
        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();

        List<BulkOperationWrapper> knownOperations = new ArrayList<>();

        for (int i = 0; i < 7; i++) {
            BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(generateDocument()));
            objectUnderTest.addOperation(bulkOperation);
            knownOperations.add(bulkOperation);
        }

        for (int i = 0; i < 7; i++) {
            assertThat(objectUnderTest.getOperationAt(i), equalTo(knownOperations.get(i)));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 10, 50, 100})
    void estimateSizeInBytesWithDocument_on_new_object_returns_estimated_document_size_plus_operation_overhead(long inputDocumentSize) {
        final SizedDocument document = generateDocumentWithLength(inputDocumentSize);
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(document));

        assertThat(createObjectUnderTest().estimateSizeInBytesWithDocument(bulkOperation),
                equalTo(inputDocumentSize + JavaClientAccumulatingBulkRequest.OPERATION_OVERHEAD));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 10, 50, 100})
    void estimateSizeInBytesWithDocument_on_request_with_operations_returns_estimated_document_size_plus_operation_overhead(long inputDocumentSize) {
        final SizedDocument document = generateDocumentWithLength(inputDocumentSize);
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(document));

        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();
        objectUnderTest.addOperation(new BulkOperationWrapper(createBulkOperation(generateDocumentWithLength(inputDocumentSize))));

        final long expectedSize = 2 * (inputDocumentSize + JavaClientAccumulatingBulkRequest.OPERATION_OVERHEAD);
        assertThat(objectUnderTest.estimateSizeInBytesWithDocument(bulkOperation),
                equalTo(expectedSize));
    }

    @Test
    void estimateSizeInBytesWithDocument_on_new_object_returns_operation_overhead_if_no_document() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(null));

        assertThat(createObjectUnderTest().estimateSizeInBytesWithDocument(bulkOperation),
                equalTo((long) JavaClientAccumulatingBulkRequest.OPERATION_OVERHEAD));
    }

    @Test
    void addOperation_adds_operation_to_the_BulkRequestBuilder() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(generateDocument()));

        createObjectUnderTest().addOperation(bulkOperation);

        verify(bulkRequestBuilder).operations(bulkOperation.getBulkOperation());
    }

    @Test
    void addOperation_throws_when_BulkOperation_is_not_an_index_request() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(mock(BulkOperation.class));

        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();

        assertThrows(UnsupportedOperationException.class, () -> objectUnderTest.addOperation(bulkOperation));
    }

    @Test
    void addOperation_throws_when_document_is_not_JsonSize() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(createBulkOperation(UUID.randomUUID().toString()));

        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.addOperation(bulkOperation));
    }

    @Test
    void getRequest_returns_BulkRequestBuilder_build() {
        BulkRequest expectedBulkRequest = mock(BulkRequest.class);
        when(bulkRequestBuilder.build()).thenReturn(expectedBulkRequest);

        assertThat(createObjectUnderTest().getRequest(), equalTo(expectedBulkRequest));
    }

    @Test
    void getRequest_called_multiple_times_only_builds_once_and_reuses_the_built_request() {
        BulkRequest expectedBulkRequest = mock(BulkRequest.class);
        when(bulkRequestBuilder.build()).thenReturn(expectedBulkRequest);

        final JavaClientAccumulatingBulkRequest objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getRequest(), equalTo(expectedBulkRequest));
        assertThat(objectUnderTest.getRequest(), sameInstance(objectUnderTest.getRequest()));

        verify(bulkRequestBuilder, times(1)).build();
    }

    private BulkOperation createBulkOperation(Object document) {
        final IndexOperation indexOperation = mock(IndexOperation.class);
        when(indexOperation.document()).thenReturn(document);
        final BulkOperation bulkOperation = mock(BulkOperation.class);
        when(bulkOperation.isIndex()).thenReturn(true);
        when(bulkOperation.index()).thenReturn(indexOperation);

        return bulkOperation;
    }

    private SizedDocument generateDocument() {
        return generateDocumentWithLength(10L);
    }

    private SizedDocument generateDocumentWithLength(long documentLength) {
        final SizedDocument sizedDocument = mock(SizedDocument.class);
        when(sizedDocument.getDocumentSize()).thenReturn(documentLength);
        return sizedDocument;
    }
}
