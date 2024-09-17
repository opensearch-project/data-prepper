/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.equalTo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.notNullValue;


@ExtendWith(MockitoExtension.class)
class AuthConfigTest {

    @Mock
    AuthConfig authConfig;

    @Mock
    AuthConfig.SaslAuthConfig saslAuthConfig;

    @Mock
    AwsIamAuthConfig testAwsIamAuthConfig;

    @Mock
    OAuthConfig testOAuthConfig;

    @Mock
    ScramAuthConfig testScramConfig;

    @Mock
    PlainTextAuthConfig testPlainTextConfig;

    @BeforeEach
    void setUp() throws IOException {
        authConfig = new AuthConfig();
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        ObjectMapper mapper = new ObjectMapper();
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            KafkaSourceConfig kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            authConfig = kafkaSourceConfig.getAuthConfig();
        }
    }

    @Test
    void testPlainTextAuthConfig() {
        assertThat(authConfig, notNullValue());
        assertThat(authConfig.getSaslAuthConfig(), notNullValue());
        assertThat(authConfig.getSaslAuthConfig().getPlainTextAuthConfig(), notNullValue());
        assertThat(authConfig.getSaslAuthConfig().getPlainTextAuthConfig(), hasProperty("username"));
        assertThat(authConfig.getSaslAuthConfig().getPlainTextAuthConfig(), hasProperty("password"));
        assertThat(authConfig.getSaslAuthConfig().getOAuthConfig(), notNullValue());
    }

    @Test
    void testScramAuthConfig() {
        assertThat(authConfig, notNullValue());
        assertThat(authConfig.getSaslAuthConfig(), notNullValue());
        assertThat(authConfig.getSaslAuthConfig().getScramAuthConfig(), notNullValue());
        assertThat(authConfig.getSaslAuthConfig().getScramAuthConfig(), hasProperty("username"));
        assertThat(authConfig.getSaslAuthConfig().getScramAuthConfig(), hasProperty("password"));
        assertThat(authConfig.getSaslAuthConfig().getScramAuthConfig(), hasProperty("mechanism"));
    }

    @Test
    void testSaslAuthConfigWithPlainText() throws NoSuchFieldException, IllegalAccessException {
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        testPlainTextConfig = mock(PlainTextAuthConfig.class);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(testPlainTextConfig);
        setField(AuthConfig.class, authConfig, "saslAuthConfig", saslAuthConfig);
        assertThat(authConfig.getSaslAuthConfig(), equalTo(saslAuthConfig));
        assertThat(authConfig.getSaslAuthConfig().getPlainTextAuthConfig(), equalTo(testPlainTextConfig));
    }

    @Test
    void testSaslAuthConfigWithScram() throws NoSuchFieldException, IllegalAccessException {
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        testScramConfig = mock(ScramAuthConfig.class);
        when(saslAuthConfig.getScramAuthConfig()).thenReturn(testScramConfig);
        setField(AuthConfig.class, authConfig, "saslAuthConfig", saslAuthConfig);
        assertThat(authConfig.getSaslAuthConfig(), equalTo(saslAuthConfig));
        assertThat(authConfig.getSaslAuthConfig().getScramAuthConfig(), equalTo(testScramConfig));
    }

    @Test
    void testSaslAuthConfigWithMskIam() throws NoSuchFieldException, IllegalAccessException {
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        AwsIamAuthConfig awsIamAuthConfig = AwsIamAuthConfig.ROLE;
        when(saslAuthConfig.getAwsIamAuthConfig()).thenReturn(awsIamAuthConfig);
        setField(AuthConfig.class, authConfig, "saslAuthConfig", saslAuthConfig);
        assertThat(authConfig.getSaslAuthConfig(), equalTo(saslAuthConfig));
        assertThat(authConfig.getSaslAuthConfig().getAwsIamAuthConfig(), equalTo(awsIamAuthConfig));
    }

    @Test
    void testSaslAuthConfigWithOAuth() throws NoSuchFieldException, IllegalAccessException {
        saslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        testOAuthConfig = mock(OAuthConfig.class);
        when(saslAuthConfig.getOAuthConfig()).thenReturn(testOAuthConfig);
        setField(AuthConfig.class, authConfig, "saslAuthConfig", saslAuthConfig);
        assertThat(authConfig.getSaslAuthConfig(), equalTo(saslAuthConfig));
        assertThat(authConfig.getSaslAuthConfig().getOAuthConfig(), equalTo(testOAuthConfig));
    }

}
