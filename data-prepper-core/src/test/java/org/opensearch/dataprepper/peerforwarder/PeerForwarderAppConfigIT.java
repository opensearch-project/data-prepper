/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class PeerForwarderAppConfigIT {

    private PeerForwarderConfiguration createObjectUnderTest() {
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.scan(PeerForwarderConfiguration.class.getPackageName());
        applicationContext.register(PeerForwarderAppConfig.class);
        applicationContext.register(InnerAppConfig.class);
        applicationContext.refresh();

        return applicationContext.getBean(PeerForwarderConfiguration.class);
    }

    @Test
    void PeerForwarderConfiguration_default_values_test() {
        final PeerForwarderConfiguration objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getServerPort(), equalTo(21890));
        assertThat(objectUnderTest.getRequestTimeout(), equalTo(10_000));
        assertThat(objectUnderTest.getServerThreadCount(), equalTo(200));
        assertThat(objectUnderTest.getMaxConnectionCount(), equalTo(500));
        assertThat(objectUnderTest.getMaxPendingRequests(), equalTo(1024));
        assertThat(objectUnderTest.isSsl(), equalTo(false));
        assertThat(objectUnderTest.getSslCertificateFile(), equalTo(null));
        assertThat(objectUnderTest.getSslKeyFile(), equalTo(null));
        assertThat(objectUnderTest.getDiscoveryMode(), equalTo(DiscoveryMode.LOCAL_NODE));
        assertThat(objectUnderTest.getClientThreadCount(), equalTo(200));
        assertThat(objectUnderTest.getBatchSize(), equalTo(48));
        assertThat(objectUnderTest.getBufferSize(), equalTo(512));
    }

    @Configuration
    static class InnerAppConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }

}
