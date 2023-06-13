/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public class CreatePointInTimeResponse {

    private final String pitId;
    private final Long pitCreationTime;

    public String getPitId() {
        return pitId;
    }

    public Long getPitCreationTime() { return pitCreationTime; }

    private CreatePointInTimeResponse(final CreatePointInTimeResponse.Builder builder) {
        this.pitId = builder.pitId;
        this.pitCreationTime = builder.pitCreationTime;
    }

    public static CreatePointInTimeResponse.Builder builder() {
        return new CreatePointInTimeResponse.Builder();
    }

    public static class Builder {

        private String pitId;
        private Long pitCreationTime;

        public Builder() {

        }

        public CreatePointInTimeResponse.Builder withPitId(final String pitId) {
            this.pitId = pitId;
            return this;
        }

        public CreatePointInTimeResponse.Builder withCreationTime(final Long pitCreationTime) {
            this.pitCreationTime = pitCreationTime;
            return this;
        }

        public CreatePointInTimeResponse build() {
            return new CreatePointInTimeResponse(this);
        }
    }
}
