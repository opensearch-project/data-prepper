package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import java.util.function.Consumer;

public class InlineRequestSender implements RequestSender {

    @Override
    public void sendRequest(final Consumer<AccumulatingBulkRequest> requestConsumer, final AccumulatingBulkRequest request) {
        requestConsumer.accept(request);
    }
}
