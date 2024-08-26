/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class CoreHttpServerIT {
    private static final Logger log = LoggerFactory.getLogger(CoreHttpServerIT.class);
    private static final String PIPELINE_CONFIGURATION_UNDER_TEST = "minimal-pipeline.yaml";
    private DataPrepperTestRunner dataPrepperTestRunner;

    @BeforeEach
    void setUp() {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(PIPELINE_CONFIGURATION_UNDER_TEST)
                .build();

        dataPrepperTestRunner.start();
    }

    @AfterEach
    void tearDown() {
        dataPrepperTestRunner.stop();
    }

    @Test
    void verify_list_api_is_running() {
        log.info("Making API request for test.");
        final AggregatedHttpResponse response = WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:4900")
                        .method(HttpMethod.GET)
                        .path("/list")
                        .build())
                .aggregate()
                .join();

        assertThat(response.status(), equalTo(HttpStatus.OK));
    }
}
