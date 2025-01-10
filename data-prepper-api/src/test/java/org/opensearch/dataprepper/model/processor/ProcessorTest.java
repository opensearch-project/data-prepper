/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.processor;

import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessorTest {
    
    @Test
    public void testDefault() {
        Processor processor = mock(Processor.class);
        when(processor.holdsEvents()).thenCallRealMethod();
        assertThat(processor.holdsEvents(), equalTo(false));
    }
}

