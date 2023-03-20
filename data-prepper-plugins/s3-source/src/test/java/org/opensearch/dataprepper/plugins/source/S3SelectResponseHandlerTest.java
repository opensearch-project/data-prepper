/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.junit.Test;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class S3SelectResponseHandlerTest {
    @Test
    public void testS3SelectResponseHandlerWithMockedResponse() throws IOException {
        S3SelectResponseHandler responseHandler = new S3SelectResponseHandler();
        final SdkPublisher publisher = mock(SdkPublisher.class);
        responseHandler.onEventStream(publisher);
        responseHandler.responseReceived(mock(SelectObjectContentResponse.class));
        responseHandler.complete();
        final List<SelectObjectContentEventStream> receivedEvents =
                responseHandler.getReceivedEvents();
        assertNotNull(receivedEvents);
    }

    @Test
    public void testS3SelectResponseHandler() throws IOException {
        S3SelectResponseHandler responseHandler = new S3SelectResponseHandler();
        final RuntimeException exception = mock(RuntimeException.class);
        responseHandler.exceptionOccurred(exception);
        responseHandler.complete();
        final List<SelectObjectContentEventStream> receivedEvents =
                responseHandler.getReceivedEvents();
        assertNotNull(receivedEvents);
    }

}
