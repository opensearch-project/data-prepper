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

@JsonInclude(JsonInclude.Include.NON_NULL)
public class HecResponse {

    @JsonProperty("text")
    private final String text;

    @JsonProperty("code")
    private final int code;

    @JsonProperty("ackId")
    private final Long ackId;

    @JsonProperty("invalid-event-number")
    private final Integer invalidEventNumber;

    private HecResponse(final String text, final int code, final Long ackId, final Integer invalidEventNumber) {
        this.text = text;
        this.code = code;
        this.ackId = ackId;
        this.invalidEventNumber = invalidEventNumber;
    }

    public static HecResponse success() {
        return new HecResponse(HecResponseCode.SUCCESS.getText(), HecResponseCode.SUCCESS.getCode(), null, null);
    }

    public static HecResponse successWithAckId(final long ackId) {
        return new HecResponse(HecResponseCode.SUCCESS.getText(), HecResponseCode.SUCCESS.getCode(), ackId, null);
    }

    public static HecResponse error(final HecResponseCode responseCode) {
        return new HecResponse(responseCode.getText(), responseCode.getCode(), null, null);
    }

    public static HecResponse errorWithInvalidEventNumber(final HecResponseCode responseCode, final int eventNumber) {
        return new HecResponse(responseCode.getText(), responseCode.getCode(), null, eventNumber);
    }

    public String getText() {
        return text;
    }

    public int getCode() {
        return code;
    }

    public Long getAckId() {
        return ackId;
    }

    public Integer getInvalidEventNumber() {
        return invalidEventNumber;
    }
}
