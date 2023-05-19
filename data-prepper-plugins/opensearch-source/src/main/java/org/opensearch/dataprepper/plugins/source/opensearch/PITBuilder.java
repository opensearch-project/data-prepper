/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.client.util.ObjectBuilder;

import static java.util.Objects.requireNonNull;

public  class PITBuilder implements ObjectBuilder<PITRequest> {

    protected StringBuilder index;

    protected String keepAlive;
    public final PITBuilder index(final StringBuilder index) {
        this.index = index;
        return this;
    }

    public final PITBuilder keepAlive(final String keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public PITRequest build() {
        requireNonNull(this.index, "'Index' was not set");
        return new PITRequest(this);
    }

}