/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;

import java.util.Objects;

public class RequestExceptionHandler {

    public HttpResponse handleException(final Exception e, final String message) {
        Objects.requireNonNull(message);
        if (e instanceof JsonProcessingException) {
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.ANY_TYPE, message);
        }

        return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR, MediaType.ANY_TYPE, message);
    }
}
