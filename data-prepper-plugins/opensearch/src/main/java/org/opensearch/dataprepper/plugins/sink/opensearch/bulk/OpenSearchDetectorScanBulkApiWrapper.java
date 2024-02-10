package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

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
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class OpenSearchDetectorScanBulkApiWrapper implements BulkApiWrapper {
    static final String DETECTOR_SCAN_API_METHOD = "POST";
    static final String DETECTOR_SCAN_API_PATH = "/_plugins/_security_analytics/detectors/run";
    static final String FILTER_PATH_PARAMETER_NAME = "filter_path";
    static final String FILTER_PATH_PARAMETER_VALUE = "errors,took,items.*.error,items.*.status,items.*._index,items.*._id";

    private static final JsonpMapper JSONP_MAPPER = new PreSerializedJsonpMapper();
    private static final JacksonJsonProvider JACKSON_JSON_PROVIDER = new JacksonJsonProvider();

    private final RestClient restClient;

    public OpenSearchDetectorScanBulkApiWrapper(final RestHighLevelClient restHighLevelClient) {
        this.restClient = restHighLevelClient.getLowLevelClient();
    }

    @Override
    public BulkResponse bulk(final BulkRequest request) throws IOException {
        final Request runDetectorsRequest = new Request(DETECTOR_SCAN_API_METHOD, DETECTOR_SCAN_API_PATH);
        runDetectorsRequest.addParameter(FILTER_PATH_PARAMETER_NAME, FILTER_PATH_PARAMETER_VALUE);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        runDetectorsRequest.setEntity(serializeRequestToBytes(request, baos));

        final Response response = restClient.performRequest(runDetectorsRequest);
        return BulkResponse._DESERIALIZER.deserialize(JACKSON_JSON_PROVIDER.createParser(response.getEntity().getContent()), JSONP_MAPPER);
    }

    private HttpEntity serializeRequestToBytes(final NdJsonpSerializable bulkRequest, final ByteArrayOutputStream baos) {

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
