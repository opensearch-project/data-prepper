/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.ObjectInputFilter;

@Configuration
public class PeerForwarderCodecAppConfig {
    @Bean
    public PeerForwarderCodec peerForwarderCodec(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final ObjectInputFilter objectInputFilter,
            @Qualifier("peerForwarderObjectMapper") final ObjectMapper objectMapper) {
        return peerForwarderConfiguration.getBinaryCodec() ?
                new JavaPeerForwarderCodec(objectInputFilter) : new JacksonPeerForwarderCodec(objectMapper);
    }

    @Bean(name = "peerForwarderObjectMapper")
    public ObjectMapper objectMapper() {
        final JavaTimeModule javaTimeModule = new JavaTimeModule();
        final LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(10 * 1024 * 1024); // 10MB
        final YAMLFactory yamlFactory = YAMLFactory.builder()
                .loaderOptions(loaderOptions)
                .build();
        return new ObjectMapper(yamlFactory).registerModule(javaTimeModule);
    }

    @Bean
    public ObjectInputFilter objectInputFilter() {
        final String baseModelPackage = "org.opensearch.dataprepper.model";

        final String pattern =
                "java.lang.Object;" +
                        "java.util.*;" +
                        "java.time.*;" +
                        "com.fasterxml.jackson.databind.node.*;" +
                        "org.opensearch.dataprepper.peerforwarder.model.*;" +
                        baseModelPackage + ".event.*;" +
                        baseModelPackage + ".trace.*;" +
                        baseModelPackage + ".log.*;" +
                        baseModelPackage + ".metric.*;" +
                        baseModelPackage + ".document.*;" +
                        "com.google.common.collect.ImmutableMap*;" +
                        "com.google.common.collect.RegularImmutableMap*;" +
                        "!*";

        final ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(pattern);

        return new LoggingObjectInputFilter(filter);
    }
}
