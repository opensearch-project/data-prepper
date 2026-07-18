/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

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
}
