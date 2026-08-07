/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

/**
 * Internal representation of the multiline grouping mode, determined from the configuration.
 */
enum MultilineMode {

    /**
     * A new event starts at each line matching the pattern.
     * Non-matching lines are continuations of the preceding event.
     */
    EVENT_START,

    /**
     * An event ends at each line matching the pattern (inclusive).
     * The next line begins a new event.
     */
    EVENT_END,

    /**
     * Lines matching the pattern are continuations of the previous event.
     * Non-matching lines start new events.
     */
    CONTINUATION_START,

    /**
     * Lines matching the pattern are prepended to the next event.
     * Non-matching lines complete the event.
     */
    CONTINUATION_END
}
