package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import java.util.function.Consumer;

public interface RequestSender {
    /**
     * Executes the provided request with the provided consumer
     *
     * @param requestConsumer - the consumer function of the request that performs the work to execute the request
     * @param request - the request to be consumed
     */
    void sendRequest(Consumer<AccumulatingBulkRequest> requestConsumer, AccumulatingBulkRequest request);
}
