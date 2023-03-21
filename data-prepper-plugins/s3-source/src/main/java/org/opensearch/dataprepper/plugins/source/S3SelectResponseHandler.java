/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
/**
 * A handler class that will be used for handling the data processing and AWS SDK client exceptions.  
 *
 * This class is derived from <code>SelectObjectContentResponseHandler</code> in the AWS SDKv2 for Java.
 */
public class S3SelectResponseHandler implements SelectObjectContentResponseHandler {
    SelectObjectContentResponse response;
    List<SelectObjectContentEventStream> receivedEvents = new ArrayList<>();
    Throwable exception;
    private static final Logger LOG = LoggerFactory.getLogger(S3SelectResponseHandler.class);

    @Override
    public void responseReceived(SelectObjectContentResponse response) {
        this.response = response;
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
    public void complete() {
        if(exception!=null)
            LOG.error("Error while downloading data",exception);
    }

    public List<SelectObjectContentEventStream> getReceivedEvents() {
        return receivedEvents;
    }

}
