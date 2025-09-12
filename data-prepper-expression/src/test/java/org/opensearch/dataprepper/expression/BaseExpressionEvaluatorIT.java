/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

abstract class BaseExpressionEvaluatorIT {
    
    protected AnnotationConfigApplicationContext applicationContext;

    @BeforeEach
    void beforeEach() {
        applicationContext = createApplicationContext();
    }

    protected AnnotationConfigApplicationContext createApplicationContext() {
        return new AnnotationConfigApplicationContext(TestExpressionSpringConfig.class);
    }
}