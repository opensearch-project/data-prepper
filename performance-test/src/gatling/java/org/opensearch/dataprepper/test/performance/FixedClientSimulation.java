/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance;

import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.opensearch.dataprepper.test.performance.tools.Chain;
import org.opensearch.dataprepper.test.performance.tools.Protocol;

import java.time.Duration;

public class FixedClientSimulation extends Simulation {
    private static final Integer largeBatchSize = 200;
    private static final Integer users = 10;
    private static final Duration duration = Duration.ofMinutes(5);

    ScenarioBuilder fixedScenario = CoreDsl.scenario("Slow Burn")
            .during(duration)
            .on(Chain.sendApacheCommonLogPostRequest("Post logs with large batch", largeBatchSize));

    public FixedClientSimulation()
    {
        setUp(fixedScenario.injectOpen(CoreDsl.atOnceUsers(users)))
                .protocols(Protocol.httpProtocol())
                .assertions(CoreDsl.global().requestsPerSec().gt(140.0));
    }
}
