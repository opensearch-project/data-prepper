package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Es6BulkApiWrapperTest {
    private static final String ES6_URI_PATTERN = "/%s/_doc/_bulk";

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private OpenSearchTransport openSearchTransport;

    @Mock
    private TransportOptions transportOptions;

    @Mock
    private BulkRequest bulkRequest;

    @Mock
    private BulkOperation bulkOperation;

    @Mock
    private IndexOperation indexOperation;

    @Mock
    private CreateOperation createOperation;

    @Mock
    private UpdateOperation updateOperation;

    @Mock
    private DeleteOperation deleteOperation;

    @Captor
    private ArgumentCaptor<JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse>> jsonEndpointArgumentCaptor;

    private Es6BulkApiWrapper objectUnderTest;
    private String testIndex;

    @BeforeEach
    void setUp() {
        objectUnderTest = new Es6BulkApiWrapper(openSearchClient);
        testIndex = RandomStringUtils.randomAlphabetic(5);
        lenient().when(bulkOperation.index()).thenReturn(indexOperation);
        lenient().when(bulkOperation.create()).thenReturn(createOperation);
        lenient().when(bulkOperation.update()).thenReturn(updateOperation);
        lenient().when(bulkOperation.delete()).thenReturn(deleteOperation);
        lenient().when(indexOperation.index()).thenReturn(testIndex);
        lenient().when(createOperation.index()).thenReturn(testIndex);
        lenient().when(updateOperation.index()).thenReturn(testIndex);
        lenient().when(deleteOperation.index()).thenReturn(testIndex);
    }

    @Test
    void testBulk() throws IOException {
        final String requestIndex = "test-index";
        final String expectedURI = String.format(ES6_URI_PATTERN, "test-index");
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(transportOptions);
        when(bulkRequest.index()).thenReturn(requestIndex);
        objectUnderTest.bulk(bulkRequest);
        verify(openSearchTransport).performRequest(
                any(BulkRequest.class), jsonEndpointArgumentCaptor.capture(), eq(transportOptions));
        final JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse> endpoint = jsonEndpointArgumentCaptor.getValue();
        assertThat(endpoint.requestUrl(bulkRequest), equalTo(expectedURI));
    }

    @ParameterizedTest
    @MethodSource("getTypeFlags")
    void testBulk_when_request_index_missing(final boolean isIndex, final boolean isCreate,
                                             final boolean isUpdate, final boolean isDelete) throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(transportOptions);
        when(bulkRequest.index()).thenReturn(null);
        when(bulkRequest.operations()).thenReturn(List.of(bulkOperation));
        lenient().when(bulkOperation.isIndex()).thenReturn(isIndex);
        lenient().when(bulkOperation.isCreate()).thenReturn(isCreate);
        lenient().when(bulkOperation.isUpdate()).thenReturn(isUpdate);
        lenient().when(bulkOperation.isDelete()).thenReturn(isDelete);
        objectUnderTest.bulk(bulkRequest);

        verify(openSearchTransport).performRequest(
                any(BulkRequest.class), jsonEndpointArgumentCaptor.capture(), eq(transportOptions));
        final JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse> endpoint = jsonEndpointArgumentCaptor.getValue();
        final String expectedURI = String.format(ES6_URI_PATTERN, testIndex);
        assertThat(endpoint.requestUrl(bulkRequest), equalTo(expectedURI));
    }

    private static Stream<Arguments> getTypeFlags() {
        return Stream.of(
                Arguments.of(true, false, false, false),
                Arguments.of(false, true, false, false),
                Arguments.of(false, false, true, false),
                Arguments.of(false, false, false, true)
        );
    }
}