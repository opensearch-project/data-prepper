/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.loghttp.codec.JsonCodec;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Post;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class LogHTTPService {

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final JsonCodec jsonCodec = new JsonCodec();
    private final Buffer<Record<String>> buffer;
    private final int bufferWriteTimeoutInMillis;

    public LogHTTPService(final int bufferWriteTimeoutInMillis, final Buffer<Record<String>> buffer) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
    }

    @Get("/log/ingest")
    public HttpResponse doGet(final AggregatedHttpRequest aggregatedHttpRequest) {
        return processRequest(aggregatedHttpRequest);
    }

    @Post("/log/ingest")
    public HttpResponse doPost(final AggregatedHttpRequest aggregatedHttpRequest) {
        return processRequest(aggregatedHttpRequest);
    }

    private HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) {
        List<String> jsonList;
        try {
            jsonList = jsonCodec.parse(aggregatedHttpRequest.content());
        } catch (IOException e) {
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.ANY_TYPE, "Bad request data format. Needs to be json array.");
        }
        for (String json: jsonList) {
            try {
                // TODO: switch to new API writeAll once ready
                buffer.write(new Record<>(json), bufferWriteTimeoutInMillis);
            } catch (TimeoutException e) {
                return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT, MediaType.ANY_TYPE, e.getMessage());
            }
        }
        return HttpResponse.of(HttpStatus.OK);
    }
}
