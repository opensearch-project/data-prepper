/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.ActionConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DataStreamDetector;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DataStreamIndex;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class EventActionResolverTest {

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Event event;

    @Mock
    private DataStreamDetector dataStreamDetector;

    @Mock
    private DataStreamIndex dataStreamIndex;

    @Test
    void resolveAction_returns_default_action() {
        final EventActionResolver resolver = new EventActionResolver("index", null, expressionEvaluator);
        assertThat(resolver.resolveAction(event, "my-index"), equalTo("index"));
    }

    @Test
    void resolveAction_uses_actions_list_with_matching_condition() {
        final ActionConfiguration actionConfig = mock(ActionConfiguration.class);
        when(actionConfig.getType()).thenReturn("delete");
        when(actionConfig.getWhen()).thenReturn("condition");
        when(expressionEvaluator.evaluateConditional("condition", event)).thenReturn(true);

        final EventActionResolver resolver = new EventActionResolver("index", List.of(actionConfig), expressionEvaluator);
        assertThat(resolver.resolveAction(event, "my-index"), equalTo("delete"));
    }

    @Test
    void resolveAction_evaluates_expression_in_action() {
        when(event.formatString("${action_field}", expressionEvaluator)).thenReturn("update");
        final EventActionResolver resolver = new EventActionResolver("${action_field}", null, expressionEvaluator);
        assertThat(resolver.resolveAction(event, "my-index"), equalTo("update"));
    }

    @Test
    void resolveAction_overrides_action_for_data_stream() {
        final EventActionResolver resolver = new EventActionResolver("index", null, expressionEvaluator);
        resolver.setDataStreamSupport(dataStreamDetector, dataStreamIndex);
        when(dataStreamDetector.isDataStream("my-data-stream")).thenReturn(true);
        when(dataStreamIndex.determineAction("index", "my-data-stream")).thenReturn("create");

        assertThat(resolver.resolveAction(event, "my-data-stream"), equalTo("create"));
    }

    @Test
    void resolveAction_does_not_override_for_non_data_stream() {
        final EventActionResolver resolver = new EventActionResolver("index", null, expressionEvaluator);
        resolver.setDataStreamSupport(dataStreamDetector, dataStreamIndex);
        when(dataStreamDetector.isDataStream("my-index")).thenReturn(false);

        assertThat(resolver.resolveAction(event, "my-index"), equalTo("index"));
    }

    @Test
    void isValidAction_returns_true_for_valid_action() {
        final EventActionResolver resolver = new EventActionResolver("index", null, expressionEvaluator);
        assertThat(resolver.isValidAction("index"), equalTo(true));
        assertThat(resolver.isValidAction("create"), equalTo(true));
        assertThat(resolver.isValidAction("update"), equalTo(true));
        assertThat(resolver.isValidAction("delete"), equalTo(true));
    }

    @Test
    void isValidAction_returns_false_for_invalid_action() {
        final EventActionResolver resolver = new EventActionResolver("index", null, expressionEvaluator);
        assertThat(resolver.isValidAction("invalid_action"), equalTo(false));
    }
}
