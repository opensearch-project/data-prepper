package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;

public interface BulkApiWrapper {
    BulkResponse bulk(BulkRequest request) throws Exception;
}
