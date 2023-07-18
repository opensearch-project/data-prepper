/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class BulkOperationWrapperTests {
    private static final String ID = UUID.randomUUID().toString();
    private static final String INDEX = UUID.randomUUID().toString();
    private static final String DOCUMENT = UUID.randomUUID().toString();

    private BulkOperation bulkOperation;

    BulkOperationWrapper createObjectUnderTest(final EventHandle eventHandle, BulkOperation aBulkOperation) {
        bulkOperation = Objects.isNull(aBulkOperation) ? mock(BulkOperation.class) : aBulkOperation;
        if (eventHandle == null) {
            return new BulkOperationWrapper(bulkOperation);
        }
        return new BulkOperationWrapper(bulkOperation, eventHandle);
    }

    @Test
    public void testConstructorWithOneArgument() {
        BulkOperationWrapper bulkOperationWithHandle = createObjectUnderTest(null, null);
        assertThat(bulkOperationWithHandle.getBulkOperation(), equalTo(bulkOperation));
        assertThat(bulkOperationWithHandle.getEventHandle(), equalTo(null));
    }

    @Test
    public void testConstructorWithTwoArguments() {
        EventHandle eventHandle = mock(EventHandle.class);
        BulkOperationWrapper bulkOperationWithHandle = createObjectUnderTest(eventHandle, null);
        assertThat(bulkOperationWithHandle.getBulkOperation(), equalTo(bulkOperation));
        assertThat(bulkOperationWithHandle.getEventHandle(), equalTo(eventHandle));
    }

    @ParameterizedTest
    @MethodSource("bulkOperationProvider")
    public void testGetId(final BulkOperation bulkOperation) {
        final BulkOperationWrapper bulkOperationWrapper = createObjectUnderTest(null, bulkOperation);
        assertThat(bulkOperationWrapper.getId(), equalTo(ID));
    }

    @Test
    public void testGetIdUnsupportedAction() {
        final BulkOperationWrapper bulkOperationWrapper = createObjectUnderTest(null, getDeleteBulkOperation());
        assertThrows(UnsupportedOperationException.class, bulkOperationWrapper::getId);
    }

    @ParameterizedTest
    @MethodSource("bulkOperationProvider")
    public void testGetIndex(final BulkOperation bulkOperation) {
        final BulkOperationWrapper bulkOperationWrapper = createObjectUnderTest(null, bulkOperation);
        assertThat(bulkOperationWrapper.getIndex(), equalTo(INDEX));
    }

    @Test
    public void testGetIndexUnsupportedAction() {
        final BulkOperationWrapper bulkOperationWrapper = createObjectUnderTest(null, getDeleteBulkOperation());
        assertThrows(UnsupportedOperationException.class, bulkOperationWrapper::getIndex);
    }

    @ParameterizedTest
    @MethodSource("bulkOperationProvider")
    public void testGetDocument(final BulkOperation bulkOperation) {
        final BulkOperationWrapper bulkOperationWrapper = createObjectUnderTest(null, bulkOperation);
        assertThat(bulkOperationWrapper.getDocument(), equalTo(DOCUMENT));
    }

    @Test
    public void testGetDocumentUnsupportedAction() {
        final BulkOperationWrapper bulkOperationWrapper = createObjectUnderTest(null, getDeleteBulkOperation());
        assertThrows(UnsupportedOperationException.class, bulkOperationWrapper::getDocument);
    }

    private static Stream<Arguments> bulkOperationProvider() {
        final IndexOperation indexOperation = new IndexOperation.Builder<>()
                .id(ID)
                .index(INDEX)
                .document(DOCUMENT)
                .build();
        final BulkOperation indexBulkOperation = (BulkOperation) new BulkOperation.Builder()
                .index(indexOperation)
                .build();

        final CreateOperation createOperation = new CreateOperation.Builder<>()
                .id(ID)
                .index(INDEX)
                .document(DOCUMENT)
                .build();
        final BulkOperation createBulkOperation = (BulkOperation) new BulkOperation.Builder()
                .create(createOperation)
                .build();

        return Stream.of(
                Arguments.of(
                        indexBulkOperation,
                        createBulkOperation
                )
        );
    }

    private BulkOperation getDeleteBulkOperation() {
        final DeleteOperation deleteOperation = new DeleteOperation.Builder()
                .id(ID)
                .index(INDEX)
                .build();
        return new BulkOperation.Builder()
                .delete(deleteOperation)
                .build();
    }
}
