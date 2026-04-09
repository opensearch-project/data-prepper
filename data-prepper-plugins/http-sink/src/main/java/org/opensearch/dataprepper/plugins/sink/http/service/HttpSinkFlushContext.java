/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.sink.http.HttpSinkSender;

public class HttpSinkFlushContext implements SinkFlushContext {
    private final HttpSinkSender httpSender;
    private final OutputCodec codec;
    private final OutputCodecContext codecContext;

    public HttpSinkFlushContext(final HttpSinkSender httpSender, final OutputCodec codec, final OutputCodecContext codecContext) {
        this.httpSender = httpSender;
        this.codec = codec;
        this.codecContext = codecContext;
    }

    public HttpSinkSender getHttpSender() {
        return httpSender;
    }

    public OutputCodec getCodec() {
        return codec;
    }

    public OutputCodecContext getCodecContext() {
        return codecContext;
    }
}
