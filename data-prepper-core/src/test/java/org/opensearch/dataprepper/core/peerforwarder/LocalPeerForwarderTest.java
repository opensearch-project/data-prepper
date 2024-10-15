/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

class LocalPeerForwarderTest {

    @Mock
    private Record<Event> record;

    @Test
    void forwardRecords_should_return_same_records() {
        List<Record<Event>> testData = Collections.singletonList(record);

        final LocalPeerForwarder localPeerForwarder = new LocalPeerForwarder();
        final Collection<Record<Event>> records = localPeerForwarder.forwardRecords(testData);

        assertThat(records, equalTo(testData));
    }

    @Test
    void receiveRecords_should_return_empty_collection() {
        final LocalPeerForwarder localPeerForwarder = new LocalPeerForwarder();
        final Collection<Record<Event>> records = localPeerForwarder.receiveRecords();

        assertThat(records.size(), equalTo(0));
        assertThat(records, is(empty()));
    }

}
