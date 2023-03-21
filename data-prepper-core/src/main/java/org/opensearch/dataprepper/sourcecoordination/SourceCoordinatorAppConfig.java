/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SourceCoordinatorAppConfig {

    @Bean
    public SourceCoordinatorFactory provideSourceCoordinatorFactory() {
        return new SourceCoordinatorFactory();
    }
}
