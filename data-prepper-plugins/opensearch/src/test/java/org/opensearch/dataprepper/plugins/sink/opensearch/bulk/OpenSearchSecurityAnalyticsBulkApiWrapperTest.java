package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.when;

public class OpenSearchSecurityAnalyticsBulkApiWrapperTest {
    @Mock
    private RestHighLevelClient restHighLevelClient;
    @Mock
    private RestClient restClient;

    private OpenSearchSecurityAnalyticsBulkApiWrapper wrapper;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        this.wrapper = new OpenSearchSecurityAnalyticsBulkApiWrapper(restHighLevelClient);
    }

    @Test
    void testSer() throws IOException {
        final String docString = "{\"test\":\"doc\"}";

        final IndexOperation.Builder<Object> indexOperationBuilder =
                new IndexOperation.Builder<>()
                        .index("test-index")
                        .document(new SerializedJsonImpl(docString.getBytes(StandardCharsets.UTF_8)));
        final BulkOperation bulkOperation = new BulkOperation.Builder()
                .index(indexOperationBuilder.build())
                .build();
        final BulkRequest bulkRequest = new BulkRequest.Builder().operations(bulkOperation).build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final HttpEntity res = wrapper.serializeRequestToBytes(bulkRequest, baos);
        System.out.println(new String(res.getContent().readAllBytes(), StandardCharsets.UTF_8));
    }
}
