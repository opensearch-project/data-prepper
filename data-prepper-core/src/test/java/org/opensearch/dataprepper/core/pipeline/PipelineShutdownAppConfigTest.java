/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.core.pipeline.PipelineShutdownAppConfig;
import org.opensearch.dataprepper.core.pipeline.PipelineShutdownOption;

import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineShutdownAppConfigTest {
    @Mock
    private DataPrepperConfiguration dataPrepperConfiguration;

    @Mock
    private PipelineShutdownOption pipelineShutdownOption;

    private PipelineShutdownAppConfig createObjectUnderTest() {
        return new PipelineShutdownAppConfig();
    }

    @Test
    void shouldShutdownOnPipelineFailurePredicate_should_return_pipelineShutdown_predicate() {
        final Predicate<Map<String, Pipeline>> predicate = mock(Predicate.class);
        when(pipelineShutdownOption.getShouldShutdownOnPipelineFailurePredicate()).thenReturn(predicate);
        when(dataPrepperConfiguration.getPipelineShutdown()).thenReturn(pipelineShutdownOption);

        assertThat(createObjectUnderTest().shouldShutdownOnPipelineFailurePredicate(dataPrepperConfiguration),
                equalTo(predicate));
    }
}