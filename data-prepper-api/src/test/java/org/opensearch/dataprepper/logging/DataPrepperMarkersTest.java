package org.opensearch.dataprepper.logging;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT_MARKER;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE_MARKER;

class DataPrepperMarkersTest {
    @Test
    void testMarkers() {
        assertThat(EVENT_MARKER.contains(SENSITIVE_MARKER.getName()), is(true));
    }
}