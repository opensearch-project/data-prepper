/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.jupiter.api.Test;

class DefaultEventHandleTests {
    AcknowledgementSet acknowledgementSet;

    @Test
    void testBasic() {
        DefaultEventHandle eventHandle = new DefaultEventHandle(acknowledgementSet);
        assertThat(eventHandle.getAcknowledgementSet(), equalTo(acknowledgementSet));
    }
}
