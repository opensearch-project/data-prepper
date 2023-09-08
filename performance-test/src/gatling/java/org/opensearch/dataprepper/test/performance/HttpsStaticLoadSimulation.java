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

public class HttpsStaticLoadSimulation extends Simulation {
    private static final Duration testDuration =  Duration.ofMinutes(5);

    ScenarioBuilder httpStaticLoad = CoreDsl.scenario("Http Static Load")
            .during(testDuration)
            .on(Chain.sendApacheCommonLogPostRequest("Average log post request", 20));

    public HttpsStaticLoadSimulation()
    {
        setUp(httpStaticLoad.injectOpen(
                CoreDsl.rampUsers(10).during(Duration.ofSeconds(10)),
                CoreDsl.atOnceUsers(10)
        ))
                .protocols(Protocol.httpsProtocol(2022))
                .maxDuration(testDuration)
                .assertions(
                        CoreDsl.global().failedRequests().percent().lt(1.0),
                        CoreDsl.global().responseTime().mean().lt(200),
                        CoreDsl.global().responseTime().max().lt(1000)
                );
    }
}
