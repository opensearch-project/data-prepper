package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.BeforeEach;
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
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.bulk.Es6BulkApiWrapper.DUMMY_DEFAULT_INDEX;

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

    @Captor
    private ArgumentCaptor<JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse>> jsonEndpointArgumentCaptor;

    private Es6BulkApiWrapper objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new Es6BulkApiWrapper(openSearchClient);
    }

    @ParameterizedTest
    @MethodSource("getIndexArguments")
    void testBulkForEs6(final String requestIndex, final String expectedURI) throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClient._transportOptions()).thenReturn(transportOptions);
        when(bulkRequest.index()).thenReturn(requestIndex);
        objectUnderTest.bulk(bulkRequest);
        verify(openSearchTransport).performRequest(
                any(BulkRequest.class), jsonEndpointArgumentCaptor.capture(), eq(transportOptions));
        final JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse> endpoint = jsonEndpointArgumentCaptor.getValue();
        assertThat(endpoint.requestUrl(bulkRequest), equalTo(expectedURI));
    }

    private static Stream<Arguments> getIndexArguments() {
        return Stream.of(
                Arguments.of(null, String.format(ES6_URI_PATTERN, DUMMY_DEFAULT_INDEX)),
                Arguments.of("test-index", String.format(ES6_URI_PATTERN, "test-index")));
    }
}