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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class LogHTTPService {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAP_TYPE_REFERENCE =
            new TypeReference<List<Map<String, Object>>>() {};
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
        List<String> jsonList = new ArrayList<>();
        try {
            // TODO: 1. move the logic to a separate codec class; 2. replace string with data model for json
            final List<Map<String, Object>> logList = mapper.readValue(aggregatedHttpRequest.content().toInputStream(),
                    LIST_OF_MAP_TYPE_REFERENCE);
            for (final Map<String, Object> log: logList) {
                jsonList.add(mapper.writeValueAsString(log));
            }
        } catch (IOException e) {
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.ANY_TYPE, e.getMessage());
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
