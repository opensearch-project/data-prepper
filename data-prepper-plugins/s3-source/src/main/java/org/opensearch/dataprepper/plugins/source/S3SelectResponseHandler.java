/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

/**
 * A handler class that will be used for handling the data processing and AWS SDK client exceptions.
 * This class is derived from <code>SelectObjectContentResponseHandler</code> in the AWS SDKv2 for Java.
 */
public class S3SelectResponseHandler implements SelectObjectContentResponseHandler {
    private List<SelectObjectContentEventStream> receivedEvents = new ArrayList<>();
    private Throwable exception;
    @Override
    public void responseReceived(SelectObjectContentResponse response) {
    }

    @Override
    public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
        publisher.subscribe(receivedEvents::add);
    }

    @Override
    public void exceptionOccurred(Throwable throwable) {
        this.exception = throwable;
    }

    @Override
    public void complete() {}

    public List<SelectObjectContentEventStream> getS3SelectContentEvents() {
        return receivedEvents;
    }

    public Throwable getException(){
        return exception;
    }

}
