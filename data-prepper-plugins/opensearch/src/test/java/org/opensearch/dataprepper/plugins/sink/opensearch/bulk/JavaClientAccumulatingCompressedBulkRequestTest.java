/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JavaClientAccumulatingCompressedBulkRequestTest {

    private BulkRequest.Builder bulkRequestBuilder;
    private OpenSearchSink sink;

    @BeforeEach
    void setUp() {
        bulkRequestBuilder = mock(BulkRequest.Builder.class);
        sink = mock(OpenSearchSink.class);

        when(bulkRequestBuilder.operations(any(BulkOperation.class)))
                .thenReturn(bulkRequestBuilder);

    }

    private JavaClientAccumulatingCompressedBulkRequest createObjectUnderTest() {
        return new JavaClientAccumulatingCompressedBulkRequest(bulkRequestBuilder, 5 * 1024 * 1024, 2, 1);
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

        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(generateDocument()));
        assertThrows(UnsupportedOperationException.class, () -> operations.add(bulkOperation));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10})
    void getOperationsCount_returns_the_correct_operation_count(final int operationCount) {
        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();
        for (int i = 0; i < operationCount; i++) {
            final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(generateDocument()));
            objectUnderTest.addOperation(bulkOperation);
        }

        assertThat(objectUnderTest.getOperationsCount(), equalTo(operationCount));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10})
    void getEstimatedSizeInBytes_returns_the_current_size(final int operationCount) throws Exception {
        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();
        final long arbitraryDocumentSize = 175;
        long expectedDocumentSize = 0;
        for (int i = 0; i < operationCount; i++) {
            final SizedDocument document = generateDocumentWithLength(arbitraryDocumentSize);
            if (i == 0) {
                expectedDocumentSize = getDocumentExpectedLength(document);
            }
            final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(document));
            objectUnderTest.addOperation(bulkOperation);
        }

        final long expectedSize = operationCount * (expectedDocumentSize);
        assertThat(objectUnderTest.getEstimatedSizeInBytes(), equalTo(expectedSize));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 10})
    void getEstimatedSizeInBytes_returns_the_operation_overhead_if_requests_have_no_documents(final int operationCount) throws Exception {
        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();
        final SizedDocument emptyDocument = generateDocumentWithLength(0);
        final long expectedDocumentSize = getDocumentExpectedLength(emptyDocument);
        for (int i = 0; i < operationCount; i++) {
            objectUnderTest.addOperation(new BulkOperationWrapper(sink, createBulkOperation(emptyDocument)));
        }

        final long expectedSize = expectedDocumentSize * operationCount;
        assertThat(objectUnderTest.getEstimatedSizeInBytes(), equalTo(expectedSize));
    }

    @Test
    void getOperationAt_returns_the_correct_index() {
        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();

        List<BulkOperationWrapper> knownOperations = new ArrayList<>();

        for (int i = 0; i < 7; i++) {
            BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(generateDocument()));
            objectUnderTest.addOperation(bulkOperation);
            knownOperations.add(bulkOperation);
        }

        for (int i = 0; i < 7; i++) {
            assertThat(objectUnderTest.getOperationAt(i), equalTo(knownOperations.get(i)));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 10, 50, 100})
    void estimateSizeInBytesWithDocument_on_new_object_returns_estimated_document_size(long inputDocumentSize) throws Exception {
        final SizedDocument document = generateDocumentWithLength(inputDocumentSize);
        final long expectedDocumentSize = getDocumentExpectedLength(document);
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(document));

        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();
        objectUnderTest.addOperation(bulkOperation);

        final long expectedSize = 2 * expectedDocumentSize;
        assertThat(objectUnderTest.estimateSizeInBytesWithDocument(new BulkOperationWrapper(sink, createBulkOperation(generateDocumentWithLength(inputDocumentSize)))),
                equalTo(expectedSize));
    }

    @Test
    void estimateSizeInBytesWithDocument_on_new_object_returns_operation_overhead_if_no_document() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(null));

        assertThat(createObjectUnderTest().estimateSizeInBytesWithDocument(bulkOperation),
                equalTo(0L));
    }

    @Test
    void addOperation_adds_operation_to_the_BulkRequestBuilder() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(generateDocument()));

        createObjectUnderTest().addOperation(bulkOperation);

        verify(bulkRequestBuilder).operations(bulkOperation.getBulkOperation());
    }

    @Test
    void addOperation_throws_when_BulkOperation_is_not_an_index_request() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, mock(BulkOperation.class));

        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();

        assertThrows(UnsupportedOperationException.class, () -> objectUnderTest.addOperation(bulkOperation));
    }

    @Test
    void addOperation_throws_when_document_is_not_Serializable() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(new Object()));

        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.addOperation(bulkOperation));
    }

    @Test
    void addOperation_does_not_throw_when_document_is_null() {
        final BulkOperationWrapper bulkOperation = new BulkOperationWrapper(sink, createBulkOperation(null));

        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();

        assertDoesNotThrow(() -> objectUnderTest.addOperation(bulkOperation));
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

        final JavaClientAccumulatingCompressedBulkRequest objectUnderTest = createObjectUnderTest();

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
        final String documentContent = RandomStringUtils.randomAlphabetic((int) documentLength);
        final byte[] documentBytes = documentContent.getBytes();

        return new SerializedJsonImpl(documentBytes);
    }

    private long getDocumentExpectedLength(final SizedDocument sizedDocument) throws Exception {
        final List<Object> docList = new ArrayList<>();
        docList.add(sizedDocument);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        final ObjectOutputStream objectOut = new ObjectOutputStream(gzipOut);
        objectOut.writeObject(docList);
        objectOut.close();

        return baos.toByteArray().length;
    }
}
