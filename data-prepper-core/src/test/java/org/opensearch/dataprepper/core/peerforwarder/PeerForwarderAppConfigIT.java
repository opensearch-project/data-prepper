/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.core.event.EventFactoryApplicationContextMarker;
import org.opensearch.dataprepper.core.peerforwarder.discovery.DiscoveryMode;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration.DEFAULT_CERTIFICATE_FILE_PATH;
import static org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration.DEFAULT_PRIVATE_KEY_FILE_PATH;

class PeerForwarderAppConfigIT {

    private PeerForwarderConfiguration createObjectUnderTest() {
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.scan(PeerForwarderConfiguration.class.getPackageName());
        applicationContext.register(PeerForwarderAppConfig.class);
        applicationContext.register(InnerAppConfig.class);
        applicationContext.scan(EventFactoryApplicationContextMarker.class.getPackage().getName());
        applicationContext.scan(DefaultAcknowledgementSetManager.class.getPackage().getName());

        applicationContext.refresh();

        return applicationContext.getBean(PeerForwarderConfiguration.class);
    }

    @Test
    void PeerForwarderConfiguration_default_values_test() {
        final PeerForwarderConfiguration objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getServerPort(), equalTo(4994));
        assertThat(objectUnderTest.getRequestTimeout(), equalTo(10_000));
        assertThat(objectUnderTest.getServerThreadCount(), equalTo(200));
        assertThat(objectUnderTest.getMaxConnectionCount(), equalTo(500));
        assertThat(objectUnderTest.getMaxPendingRequests(), equalTo(1024));
        assertThat(objectUnderTest.isSsl(), equalTo(true));
        assertThat(objectUnderTest.getSslCertificateFile(), equalTo(DEFAULT_CERTIFICATE_FILE_PATH));
        assertThat(objectUnderTest.getSslKeyFile(), equalTo(DEFAULT_PRIVATE_KEY_FILE_PATH));
        assertThat(objectUnderTest.getDiscoveryMode(), equalTo(DiscoveryMode.LOCAL_NODE));
        assertThat(objectUnderTest.getClientThreadCount(), equalTo(200));
        assertThat(objectUnderTest.getBatchSize(), equalTo(48));
        assertThat(objectUnderTest.getBufferSize(), equalTo(512));
    }

    @Configuration
    static class InnerAppConfig {

        @Bean
        YAMLFactory yamlFactory() {
            return new YAMLFactory();
        }
    }

}
