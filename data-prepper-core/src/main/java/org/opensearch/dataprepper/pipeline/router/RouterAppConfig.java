/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

@Configuration
class RouterAppConfig {
    @Bean
    public RouterFactory routeEvaluatorFactory(final ExpressionEvaluator<Boolean> expressionEvaluator,
            final EventFactory eventFactory, final AcknowledgementSetManager acknowledgementSetManager) {
        return new RouterFactory(expressionEvaluator, eventFactory, acknowledgementSetManager);
    }
}
