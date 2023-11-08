/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public class DeletePointInTimeRequest {
    private final String pitId;

    public String getPitId() {
        return pitId;
    }

    private DeletePointInTimeRequest(final DeletePointInTimeRequest.Builder builder) {
        this.pitId = builder.pitId;
    }

    public static DeletePointInTimeRequest.Builder builder() {
        return new DeletePointInTimeRequest.Builder();
    }

    public static class Builder {

        private String pitId;

        public Builder() {

        }

        public DeletePointInTimeRequest.Builder withPitId(final String pitId) {
            this.pitId = pitId;
            return this;
        }

        public DeletePointInTimeRequest build() {
            return new DeletePointInTimeRequest(this);
        }
    }
}
