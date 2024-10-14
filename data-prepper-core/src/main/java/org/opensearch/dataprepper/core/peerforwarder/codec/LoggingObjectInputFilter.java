/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputFilter;
import java.util.Objects;

/**
 * A decorator for {@link ObjectInputFilter} which logs information when the filter is rejected.
 */
class LoggingObjectInputFilter implements ObjectInputFilter {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingObjectInputFilter.class);
    private final ObjectInputFilter filter;

    public LoggingObjectInputFilter(final ObjectInputFilter filter) {
        this.filter = Objects.requireNonNull(filter);
    }

    @Override
    public Status checkInput(final FilterInfo filterInfo) {
        final Status status = filter.checkInput(filterInfo);

        if(status == Status.REJECTED) {
            LOG.warn("Unable to deserialize: {}", filterInfo.serialClass());
        }

        return status;
    }
}
