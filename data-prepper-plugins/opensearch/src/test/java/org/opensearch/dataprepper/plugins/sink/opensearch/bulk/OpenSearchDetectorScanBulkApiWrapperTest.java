package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.apache.http.HttpEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.OperationType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.bulk.OpenSearchDetectorScanBulkApiWrapper.DETECTOR_SCAN_API_METHOD;
import static org.opensearch.dataprepper.plugins.sink.opensearch.bulk.OpenSearchDetectorScanBulkApiWrapper.DETECTOR_SCAN_API_PATH;
import static org.opensearch.dataprepper.plugins.sink.opensearch.bulk.OpenSearchDetectorScanBulkApiWrapper.FILTER_PATH_PARAMETER_NAME;
import static org.opensearch.dataprepper.plugins.sink.opensearch.bulk.OpenSearchDetectorScanBulkApiWrapper.FILTER_PATH_PARAMETER_VALUE;

public class OpenSearchDetectorScanBulkApiWrapperTest {
    private static final String BULK_RESPONSE_CONTENT = "{\"took\":50,\"errors\":false,\"items\":[{\"index\":{\"_index\":" +
            "\"test-index-4\",\"_id\":\"ipUCjI0B4Le7NEG4qhe-\",\"status\":201}},{\"index\":{\"_index\":\"test-index-4\"," +
            "\"_id\":\"i5UCjI0B4Le7NEG4qhe-\",\"status\":201}}]}";
    private static final String BULK_REQUEST_CONTENT_TEMPLATE = "{\"index\":{\"_index\":\"%s\"}}\n" +
            "\"%s\"\n" +
            "{\"index\":{\"_index\":\"%s\"}}\n" +
            "\"%s\"\n";

    @Mock
    private RestHighLevelClient restHighLevelClient;
    @Mock
    private RestClient restClient;
    @Mock
    private Response response;
    @Mock
    private HttpEntity httpEntity;

    private OpenSearchDetectorScanBulkApiWrapper wrapper;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        this.wrapper = new OpenSearchDetectorScanBulkApiWrapper(restHighLevelClient);
    }

    @Test
    void testBulk() throws IOException {
        final String index1 = UUID.randomUUID().toString();
        final String document1 = UUID.randomUUID().toString();
        final IndexOperation<Object> indexOperation1 = createIndexOperation(index1, document1);
        final BulkOperation bulkOperation1 = new BulkOperation.Builder()
                .index(indexOperation1)
                .build();

        final String index2 = UUID.randomUUID().toString();
        final String document2 = UUID.randomUUID().toString();
        final IndexOperation<Object> indexOperation2 = createIndexOperation(index2, document2);
        final BulkOperation bulkOperation2 = new BulkOperation.Builder()
                .index(indexOperation2)
                .build();

        final BulkRequest bulkRequest = new BulkRequest.Builder()
                .operations(List.of(bulkOperation1, bulkOperation2))
                .build();

        when(restClient.performRequest(any())).thenReturn(response);
        when(response.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(BULK_RESPONSE_CONTENT.getBytes()));

        final BulkResponse bulkResponse = wrapper.bulk(bulkRequest);
        assertThat(bulkResponse.errors(), equalTo(false));
        assertThat(bulkResponse.took(), equalTo(50L));
        assertThat(bulkResponse.items().size(), equalTo(2));
        assertThat(bulkResponse.items().get(0).index(), equalTo("test-index-4"));
        assertThat(bulkResponse.items().get(0).error(), equalTo(null));
        assertThat(bulkResponse.items().get(0).operationType(), equalTo(OperationType.Index));
        assertThat(bulkResponse.items().get(0).id(), equalTo("ipUCjI0B4Le7NEG4qhe-"));
        assertThat(bulkResponse.items().get(0).status(), equalTo(201));
        assertThat(bulkResponse.items().get(1).index(), equalTo("test-index-4"));
        assertThat(bulkResponse.items().get(1).error(), equalTo(null));
        assertThat(bulkResponse.items().get(1).operationType(), equalTo(OperationType.Index));
        assertThat(bulkResponse.items().get(1).id(), equalTo("i5UCjI0B4Le7NEG4qhe-"));
        assertThat(bulkResponse.items().get(1).status(), equalTo(201));

        verify(restClient).performRequest(argThat(r -> {
            assertThat(r.getMethod(), equalTo(DETECTOR_SCAN_API_METHOD));
            assertThat(r.getEndpoint(), equalTo(DETECTOR_SCAN_API_PATH));
            assertThat(r.getParameters().size(), equalTo(1));
            assertThat(r.getParameters().containsKey(FILTER_PATH_PARAMETER_NAME), equalTo(true));
            assertThat(r.getParameters().get(FILTER_PATH_PARAMETER_NAME), equalTo(FILTER_PATH_PARAMETER_VALUE));

            try {
                final String contentAsString = new String(r.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
                assertThat(contentAsString, equalTo(String.format(BULK_REQUEST_CONTENT_TEMPLATE, index1, document1, index2, document2)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return true;
        }));
    }

    private IndexOperation<Object> createIndexOperation(final String indexName, final String document) {
        return new IndexOperation.Builder<>()
                        .index(indexName)
                        .document(document)
                        .build();
    }
}
