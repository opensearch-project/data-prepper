/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.opensearch.dataprepper.model.codec.OutputCodec;

import java.util.Collection;
import java.util.List;
public class HttpSinkService {

    private final HttpSinkConfiguration httpSinkConf;

    private final BufferFactory bufferFactory;

    private final List<HttpAuthOptions> httpAuthOptions;
    private OutputCodec codec;

    public HttpSinkService(final OutputCodec codec,
                           final HttpSinkConfiguration httpSinkConf,
                           final BufferFactory bufferFactory,
                           final List<HttpAuthOptions> httpAuthOptions){
        this.codec= codec;
        this.httpSinkConf = httpSinkConf;
        this.bufferFactory = bufferFactory;
        this.httpAuthOptions = httpAuthOptions;
    }

    public void processRecords(Collection<Record<Event>> records) {
        records.forEach(record -> {
            try{
                // logic to fetch the records in batch as per threshold limit -  checkThresholdExceed();
                // apply the codec
                // push to http end point
            }catch(Exception e){
                // In case of any exception, need to write the exception in dlq  - logFailureForDlqObjects();
                // In case of any exception, need to push the web hook url- logFailureForWebHook();
            }
            //TODO: implement end to end acknowledgement
        });
    }

    public static boolean checkThresholdExceed(final Buffer currentBuffer,
                                               final int maxEvents,
                                               final ByteCount maxBytes,
                                               final long maxCollectionDuration) {
        // logic for checking the threshold
        return true;
    }

}
