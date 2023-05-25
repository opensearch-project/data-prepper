/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.model;

import org.opensearch.client.util.ObjectBuilder;

import static java.util.Objects.requireNonNull;

public  class PitBuilder implements ObjectBuilder<PitRequest> {

    protected StringBuilder index;

    protected String keepAlive;
    public final PitBuilder index(final StringBuilder index) {
        this.index = index;
        return this;
    }

    public final PitBuilder keepAlive(final String keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public PitRequest build() {
        requireNonNull(this.index, "'Index' was not set");
        return new PitRequest(this);
    }

}