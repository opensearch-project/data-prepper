/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpDsl;
import org.opensearch.dataprepper.test.performance.tools.PathTarget;
import org.opensearch.dataprepper.test.performance.tools.Protocol;
import org.opensearch.dataprepper.test.performance.tools.Templates;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class VariousGrokPatternsSimulation extends Simulation {
    private static final Integer rampUsers = 20;
    private static final Duration rampUpTime = Duration.ofSeconds(30);
    private static final Duration testDuration = Duration.ofMinutes(10);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static Map<String, String> logObject(final String logValue) {
        final Map<String, String> log = new HashMap<>();
        log.put("log", logValue);
        return log;
    }

    private static final Function<Session, String> multipleGrokPatterns = session -> {
        List<Map<String, String>> logData = new ArrayList<>(5);
        logData.add(logObject("127.0.0.1 - Marita [" + Templates.now() + "] \\\"GET /apache_pb.gif HTTP/1.0\\\" 200 2326"));
        logData.add(logObject("127.0.0.1 - Rosaline [" + Templates.now() + "] \\\"PUT /apache_pb.gif HTTP/1.0\\\" 202 2326"));
        logData.add(logObject("127.0.0.1 - Talbot [" + Templates.now() + "] \\\"POST /apache_pb.gif HTTP/1.0\\\" 400 2326"));
        logData.add(logObject("127.0.0.1 - Adriene [" + Templates.now() + "] \\\"DELETE /apache_pb.gif HTTP/1.0\\\" 404 2326"));
        logData.add(logObject("I should fail the grok parser"));

        try {
            return mapper.writer().writeValueAsString(logData);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    };

    ChainBuilder sendMultipleGrokPatterns = CoreDsl.exec(
            HttpDsl.http("Http multiple grok pattern request")
                    .post(PathTarget.getPath())
                    .asJson()
                    .body(CoreDsl.StringBody(VariousGrokPatternsSimulation.multipleGrokPatterns)));

    ScenarioBuilder sendMultipleGrokPatternsScenario = CoreDsl.scenario("Send multiple grok patterns")
            .during(testDuration)
            .on(sendMultipleGrokPatterns);

    {
        setUp(sendMultipleGrokPatternsScenario.injectOpen(
                CoreDsl.rampUsers(rampUsers).during(rampUpTime)
        )).protocols(Protocol.httpProtocol())
                .assertions(
                        CoreDsl.global().responseTime().max().lt(1000),
                        CoreDsl.global().failedRequests().count().is(0L)
                );
    }
}
