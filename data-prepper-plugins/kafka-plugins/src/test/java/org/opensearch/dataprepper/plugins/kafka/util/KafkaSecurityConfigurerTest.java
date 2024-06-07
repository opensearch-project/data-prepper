package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicBasicCredentialsProvider;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicSaslClientCallbackHandler;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.common.config.SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaSecurityConfigurerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSecurityConfigurerTest.class);

    @Mock
    private PluginConfigObservable pluginConfigObservable;
    @Mock
    private DynamicBasicCredentialsProvider dynamicBasicCredentialsProvider;
    @Mock
    private KafkaConnectionConfig kafkaConnectionConfig;
    @Mock
    private AuthConfig authConfig;
    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;
    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;
    @Captor
    private ArgumentCaptor<PluginConfigObserver> pluginConfigObserverArgumentCaptor;

    @Test
    public void testSetAuthPropertiesWithSaslPlainCertificate() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-certificate-content.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is("CERTIFICATE_DATA"));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(CustomClientSslEngineFactory.class));
    }

    @Test
    public void testSetAuthPropertiesWithNoAuthSsl() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-no-auth-ssl.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is("SSL"));
        assertThat(props.getProperty("certificateContent"), is("CERTIFICATE_DATA"));
        assertThat(props.get("ssl.engine.factory.class"), is(CustomClientSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesWithNoAuthSslNone() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-no-auth-ssl-none.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is(nullValue()));
        assertThat(props.getProperty("security.protocol"), is(nullValue()));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesWithNoAuthInsecure() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-auth-insecure.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_PLAINTEXT"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(InsecureSslEngineFactory.class));
    }
    @Test
    public void testSetAuthPropertiesAuthSslWithTrustStore() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-truststore.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is("some-file-path"));
        assertThat(props.getProperty("ssl.truststore.password"), is("some-password"));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesAuthSslWithNoCertContentNoTrustStore() throws Exception {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-sasl-ssl-no-cert-content-no-truststore.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("sasl.mechanism"), is("PLAIN"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
    }

    @Test
    public void testSetAuthPropertiesBootstrapServersWithSaslIAM() throws IOException {
        final Properties props = new Properties();
        final KafkaSourceConfig kafkaSourceConfig = createKafkaSinkConfig("kafka-pipeline-bootstrap-servers-sasl-iam.yaml");
        KafkaSecurityConfigurer.setAuthProperties(props, kafkaSourceConfig, LOG);
        assertThat(props.getProperty("bootstrap.servers"), is("localhost:9092"));
        assertThat(props.getProperty("sasl.mechanism"), is("AWS_MSK_IAM"));
        assertThat(props.getProperty("sasl.jaas.config"),
                is("software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"sts_role_arn\" awsStsRegion=\"us-east-2\";"));
        assertThat(props.getProperty("security.protocol"), is("SASL_SSL"));
        assertThat(props.getProperty("certificateContent"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.location"), is(nullValue()));
        assertThat(props.getProperty("ssl.truststore.password"), is(nullValue()));
        assertThat(props.get("ssl.engine.factory.class"), is(nullValue()));
        assertThat(props.get("sasl.client.callback.handler.class"),
                is("software.amazon.msk.auth.iam.IAMClientCallbackHandler"));
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNonNullPlainTextAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        final Properties properties = new Properties();
        try (final MockedStatic<DynamicBasicCredentialsProvider> dynamicBasicCredentialsProviderMockedStatic =
                     mockStatic(DynamicBasicCredentialsProvider.class)) {
            dynamicBasicCredentialsProviderMockedStatic.when(DynamicBasicCredentialsProvider::getInstance)
                    .thenReturn(dynamicBasicCredentialsProvider);
            KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                    properties, kafkaConnectionConfig, pluginConfigObservable);
        }
        assertThat(properties.get(SASL_CLIENT_CALLBACK_HANDLER_CLASS), equalTo(DynamicSaslClientCallbackHandler.class));
        verify(dynamicBasicCredentialsProvider).refresh(kafkaConnectionConfig);
        verify(pluginConfigObservable).addPluginConfigObserver(pluginConfigObserverArgumentCaptor.capture());
        final PluginConfigObserver pluginConfigObserver = pluginConfigObserverArgumentCaptor.getValue();
        final KafkaConnectionConfig newConfig = mock(KafkaConnectionConfig.class);
        pluginConfigObserver.update(newConfig);
        verify(dynamicBasicCredentialsProvider).refresh(newConfig);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullPlainTextAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullSaslAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullAuthConfig() {
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    private KafkaSourceConfig createKafkaSinkConfig(final String fileName) throws IOException {
        final Yaml yaml = new Yaml();
        final FileReader fileReader = new FileReader(Objects.requireNonNull(getClass().getClassLoader()
                .getResource(fileName)).getFile());
        final Map<String, Map<String, Map<String, Map<String, Object>>>> data = yaml.load(fileReader);
        final Map<String, Map<String, Map<String, Object>>> logPipelineMap = data.get("log-pipeline");
        final Map<String, Map<String, Object>> sourceMap = logPipelineMap.get("source");
        final Map<String, Object> kafkaConfigMap = sourceMap.get("kafka");
        final ObjectMapper mapper = new ObjectMapper();
        final String json = mapper.writeValueAsString(kafkaConfigMap);
        final Reader reader = new StringReader(json);
        return mapper.readValue(reader, KafkaSourceConfig.class);
    }
}
