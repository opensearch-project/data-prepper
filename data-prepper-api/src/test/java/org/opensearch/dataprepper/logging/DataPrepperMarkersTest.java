package org.opensearch.dataprepper.logging;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

class DataPrepperMarkersTest {
    @Test
    void testMarkers() {
        assertThat(EVENT.contains(SENSITIVE.getName()), is(true));
    }
}