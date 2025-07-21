/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.processor;

import org.opensearch.dataprepper.model.failures.FailurePipeline;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doCallRealMethod;

public class ProcessorTest {
    
    @Test
    public void testDefault() {
        Processor processor = mock(Processor.class);
        when(processor.holdsEvents()).thenCallRealMethod();
        assertThat(processor.holdsEvents(), equalTo(false));
    }

    @Test
    public void testSetFailurePipeline() {
        Processor processor = mock(Processor.class);
        FailurePipeline failurePipeline = mock(FailurePipeline.class);
        doCallRealMethod().when(processor).setFailurePipeline(failurePipeline);
        processor.setFailurePipeline(failurePipeline);
    }
}

