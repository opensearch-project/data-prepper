package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonGenerator;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.NdJsonpSerializable;
import org.opensearch.client.json.jackson.JacksonJsonProvider;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class OpenSearchSecurityAnalyticsBulkApiWrapper implements BulkApiWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSecurityAnalyticsBulkApiWrapper.class);
    private static final JsonpMapper JSONP_MAPPER = new PreSerializedJsonpMapper();
    private static final JacksonJsonProvider JACKSON_JSON_PROVIDER = new JacksonJsonProvider();

    private final RestClient restClient;

    public OpenSearchSecurityAnalyticsBulkApiWrapper(final RestHighLevelClient restHighLevelClient) {
        this.restClient = restHighLevelClient.getLowLevelClient();
    }

    @Override
    public BulkResponse bulk(final BulkRequest request) throws IOException {
        final Request runDetectorsRequest = new Request("POST", "/_plugins/_security_analytics/detectors/run");
        runDetectorsRequest.addParameter("filter_path", "errors,took,items.*.error,items.*.status,items.*._index,items.*._id");

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        runDetectorsRequest.setEntity(serializeRequestToBytes(request, baos));

        final Response response = restClient.performRequest(runDetectorsRequest);
        return BulkResponse._DESERIALIZER.deserialize(JACKSON_JSON_PROVIDER.createParser(response.getEntity().getContent()), JSONP_MAPPER);
    }

    public HttpEntity serializeRequestToBytes(final NdJsonpSerializable bulkRequest, final ByteArrayOutputStream baos) {

        final Iterator<?> values = bulkRequest._serializables();

        while (values.hasNext()) {
            Object item = values.next();
            if (item instanceof NdJsonpSerializable && item != bulkRequest) { // do not recurse on the item itself
                serializeRequestToBytes((NdJsonpSerializable) item, baos);
            } else {
                final JsonGenerator generator = JSONP_MAPPER.jsonProvider().createGenerator(baos);
                JSONP_MAPPER.serialize(item, generator);
                generator.close();
                baos.write('\n');
            }
        }

        return new ByteArrayEntity(baos.toByteArray(), ContentType.APPLICATION_JSON);
    }
}
