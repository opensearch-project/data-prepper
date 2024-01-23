/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.peerforwarder;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

import java.util.Collection;

class RequiresPeerForwardingTest {

    public class SimpleRequiresPeerForwarding implements RequiresPeerForwarding {
        @Override
        public Collection<String> getIdentificationKeys() {
            return null;
        }
    }

    @Test
    void testRequiresPeerForwardingTest() {
        Collection<Record<Event>> records = mock(Collection.class);
        RequiresPeerForwarding requiresPeerForwarding = new SimpleRequiresPeerForwarding();
        assertThat(requiresPeerForwarding.applicableEventsForPeerForwarding(records), equalTo(records));
    }
}


