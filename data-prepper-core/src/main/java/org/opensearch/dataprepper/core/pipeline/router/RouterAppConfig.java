/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.pipeline.router;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class RouterAppConfig {
    @Bean
    public RouterFactory routeEvaluatorFactory(final ExpressionEvaluator expressionEvaluator) {
        return new RouterFactory(expressionEvaluator);
    }
}
