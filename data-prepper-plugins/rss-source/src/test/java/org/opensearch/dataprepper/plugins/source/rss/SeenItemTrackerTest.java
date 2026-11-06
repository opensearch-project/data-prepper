/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SeenItemTrackerTest {

    @Test
    void addIfNew_returns_true_for_new_and_false_for_seen() {
        final SeenItemTracker tracker = new SeenItemTracker(100);
        assertThat(tracker.addIfNew("a"), equalTo(true));
        assertThat(tracker.addIfNew("a"), equalTo(false));
        assertThat(tracker.addIfNew("b"), equalTo(true));
    }

    @Test
    void evicts_eldest_when_over_capacity() {
        final SeenItemTracker tracker = new SeenItemTracker(2);
        tracker.addIfNew("a");
        tracker.addIfNew("b");
        tracker.addIfNew("c"); // evicts "a"
        assertThat(tracker.addIfNew("a"), equalTo(true));
        assertThat(tracker.addIfNew("c"), equalTo(false));
    }

    @Test
    void contains_reports_membership_without_inserting() {
        final SeenItemTracker tracker = new SeenItemTracker(100);
        assertThat(tracker.contains("a"), equalTo(false));
        assertThat(tracker.addIfNew("a"), equalTo(true));
        assertThat(tracker.contains("a"), equalTo(true));
    }

    @Test
    void constructor_rejects_non_positive_max_size() {
        // A maxSize < 1 would silently disable dedup (evict on every insert).
        assertThrows(IllegalArgumentException.class, () -> new SeenItemTracker(0));
        assertThrows(IllegalArgumentException.class, () -> new SeenItemTracker(-1));
    }
}
