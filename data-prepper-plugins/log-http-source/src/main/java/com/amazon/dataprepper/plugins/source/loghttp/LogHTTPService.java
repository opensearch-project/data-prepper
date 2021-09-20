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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Post;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LogHTTPService {

    private final Buffer<Record<String>> buffer;
    private final int bufferWriteTimeoutInMillis;

    public LogHTTPService(int bufferWriteTimeoutInMillis, Buffer<Record<String>> buffer) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
    }

    @Get("/log/ingest")
    protected HttpResponse doGet(AggregatedHttpRequest aggregatedHttpRequest) {
        return processRequest(aggregatedHttpRequest);
    }

    @Post("/log/ingest")
    protected HttpResponse doPost(AggregatedHttpRequest aggregatedHttpRequest) {
        return processRequest(aggregatedHttpRequest);
    }

    private HttpResponse processRequest(AggregatedHttpRequest aggregatedHttpRequest) {
        List<Map<String, Object>> jsonList = Collections.emptyList();
        try {
            jsonList = new ObjectMapper().readValue(aggregatedHttpRequest.content().toInputStream(),
                    new TypeReference<List<Map<String, Object>>>() {});
        } catch (IOException e) {
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.JSON, e.getMessage());
        }
        // TODO:
        //  1. wrap maps into Record<RecordMap>
        //  2. write into Buffer
        return HttpResponse.of(HttpStatus.OK);
    }
}
