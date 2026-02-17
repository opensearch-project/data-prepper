/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.performance;

import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.opensearch.dataprepper.test.performance.tools.Chain;
import org.opensearch.dataprepper.test.performance.tools.Protocol;

public class SingleRequestSimulation extends Simulation {
    ScenarioBuilder basicScenario = CoreDsl.scenario("Post single json log file")
            .exec(Chain.sendApacheCommonLogPostRequest("Post single log", 1));


    public SingleRequestSimulation()
    {

        setUp(
                basicScenario.injectOpen(CoreDsl.atOnceUsers(1))
        ).protocols(
                Protocol.httpProtocol()
        ).assertions(
                CoreDsl.global().responseTime().max().lt(1000),
                CoreDsl.global().successfulRequests().percent().is(100.0)
        );
    }
}
