/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class HecAckResponse {

    @JsonProperty("acks")
    private final Map<String, Boolean> acks;

    public HecAckResponse(final Map<String, Boolean> acks) {
        this.acks = acks;
    }

    public Map<String, Boolean> getAcks() {
        return acks;
    }
}
