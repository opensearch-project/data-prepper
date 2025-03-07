/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicSaslClientCallbackHandler;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicBasicCredentialsProvider;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.ScramAuthConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.model.StsException;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.regions.Region;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import software.amazon.awssdk.services.glue.model.Compatibility;

import org.slf4j.Logger;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * * This is static property configure dedicated to authentication related information given in pipeline.yml
 */

public class KafkaSecurityConfigurer {

    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final String SECURITY_PROTOCOL = "security.protocol";

    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    private static final String SASL_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";
    private static final String SASL_CLIENT_CALLBACK_HANDLER_CLASS = "sasl.client.callback.handler.class";

    private static final String SASL_JWKS_ENDPOINT_URL = "sasl.oauthbearer.jwks.endpoint.url";

    private static final String SASL_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url";

    private static final String PLAINTEXT_JAASCONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username= \"%s\" password=  " +
            " \"%s\";";
    private static final String OAUTH_JAASCONFIG = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='"
            + "%s" + "' clientSecret='" + "%s" + "' scope='" + "%s" + "' OAUTH_LOGIN_SERVER='" + "%s" +
            "' OAUTH_LOGIN_ENDPOINT='" + "%s" + "' OAUT_LOGIN_GRANT_TYPE=" + "%s" +
            " OAUTH_LOGIN_SCOPE=%s OAUTH_AUTHORIZATION='Basic " + "%s" + "';";

    private static final String INSTROSPECT_SERVER_PROPERTIES = " OAUTH_INTROSPECT_SERVER='"
            + "%s" + "' OAUTH_INTROSPECT_ENDPOINT='" + "%s" + "' " +
            "OAUTH_INTROSPECT_AUTHORIZATION='Basic " + "%s";

    private static final String PLAIN_MECHANISM = "PLAIN";
    private static final String OAUTHBEARER_MECHANISM = "OAUTHBEARER";

    private static final String SASL_PLAINTEXT_PROTOCOL = "SASL_PLAINTEXT";

    private static final String PLAINTEXT_PROTOCOL = "PLAINTEXT";

    private static final String REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";

    private static final int MAX_KAFKA_CLIENT_RETRIES = 360; // for one hour every 10 seconds
    private static final String SSL_ENGINE_FACTORY_CLASS = "ssl.engine.factory.class";
    private static final String CERTIFICATE_CONTENT = "certificateContent";
    private static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";

    private static AwsCredentialsProvider mskCredentialsProvider;
    private static AwsCredentialsProvider awsGlueCredentialsProvider;
    private static GlueSchemaRegistryKafkaDeserializer glueDeserializer;


    /*public static void setSaslPlainTextProperties(final KafkaSourceConfig kafkaSourConfig,
                                                  final Properties properties) {
        final AuthConfig.SaslAuthConfig saslAuthConfig = kafkaSourConfig.getAuthConfig().getSaslAuthConfig();
        String username = saslAuthConfig.getPlainTextAuthConfig().getUsername();
        String password = saslAuthConfig.getPlainTextAuthConfig().getPassword();
        if (saslAuthConfig.getPlainTextAuthConfig() != null) {
            properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
        }
        if (saslAuthConfig!= null) {
            if (StringUtils.isNotEmpty(saslAuthConfig.getPlainTextAuthConfig().getPlaintext()) &&
                    PLAINTEXT_PROTOCOL.equalsIgnoreCase(saslAuthConfig.getAuthProtocolConfig().getPlaintext())) {
                properties.put(SECURITY_PROTOCOL, PLAINTEXT_PROTOCOL);
            } else if (StringUtils.isNotEmpty(saslAuthConfig.getAuthProtocolConfig().getPlaintext()) &&
                    SASL_PLAINTEXT_PROTOCOL.equalsIgnoreCase(saslAuthConfig.getAuthProtocolConfig().getPlaintext())) {
                properties.put(SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
            }
        }
        properties.put(SASL_JAAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
    }*/

    private static void setPlainTextAuthProperties(final Properties properties, final PlainTextAuthConfig plainTextAuthConfig,
                                                   final EncryptionConfig encryptionConfig) {
        final String username = plainTextAuthConfig.getUsername();
        final String password = plainTextAuthConfig.getPassword();
        properties.put(SASL_MECHANISM, "PLAIN");
        properties.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        if (checkEncryptionType(encryptionConfig, EncryptionType.SSL)) {
            properties.put(SECURITY_PROTOCOL, "SASL_SSL");
            setSecurityProtocolSSLProperties(properties, encryptionConfig);
        } else { // EncryptionType.NONE
            properties.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        }
    }

    private static void setScramAuthProperties(final Properties properties, final ScramAuthConfig scramAuthConfig,
                                                   final EncryptionConfig encryptionConfig) {
        final String username = scramAuthConfig.getUsername();
        final String password = scramAuthConfig.getPassword();
        final String mechanism = scramAuthConfig.getMechanism();
        properties.put(SASL_MECHANISM, mechanism);
        properties.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        if (checkEncryptionType(encryptionConfig, EncryptionType.SSL)) {
            properties.put(SECURITY_PROTOCOL, "SASL_SSL");
            setSecurityProtocolSSLProperties(properties, encryptionConfig);
        } else { // EncryptionType.NONE
            properties.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        }
    }

    private static void setSecurityProtocolSSLProperties(final Properties properties, final EncryptionConfig encryptionConfig) {
        if (Objects.nonNull(encryptionConfig.getCertificate())) {
            setCustomSslProperties(properties, encryptionConfig.getCertificate());
        } else if (Objects.nonNull(encryptionConfig.getTrustStoreFilePath()) &&
                Objects.nonNull(encryptionConfig.getTrustStorePassword())) {
            setTruststoreProperties(properties, encryptionConfig);
        }
    }

    private static void setCustomSslProperties(final Properties properties, final String certificateContent) {
        properties.put(CERTIFICATE_CONTENT, certificateContent);
        properties.put(SSL_ENGINE_FACTORY_CLASS, CustomClientSslEngineFactory.class);
    }

    private static void setTruststoreProperties(final Properties properties, final EncryptionConfig encryptionConfig) {
        properties.put(SSL_TRUSTSTORE_LOCATION, encryptionConfig.getTrustStoreFilePath());
        properties.put(SSL_TRUSTSTORE_PASSWORD, encryptionConfig.getTrustStorePassword());
    }

    public static void setOauthProperties(final KafkaClusterAuthConfig kafkaClusterAuthConfig,
                                          final Properties properties) {
        final OAuthConfig oAuthConfig = kafkaClusterAuthConfig.getAuthConfig().getSaslAuthConfig().getOAuthConfig();
        final String oauthClientId = oAuthConfig.getOauthClientId();
        final String oauthClientSecret = oAuthConfig.getOauthClientSecret();
        final String oauthLoginServer = oAuthConfig.getOauthLoginServer();
        final String oauthLoginEndpoint = oAuthConfig.getOauthLoginEndpoint();
        final String oauthLoginGrantType = oAuthConfig.getOauthLoginGrantType();
        final String oauthLoginScope = oAuthConfig.getOauthLoginScope();
        final String oauthAuthorizationToken = Base64.getEncoder().encodeToString((oauthClientId + ":" + oauthClientSecret).getBytes());
        final String oauthIntrospectEndpoint = oAuthConfig.getOauthIntrospectEndpoint();
        final String tokenEndPointURL = oAuthConfig.getOauthTokenEndpointURL();
        final String saslMechanism = oAuthConfig.getOauthSaslMechanism();
        final String securityProtocol = oAuthConfig.getOauthSecurityProtocol();
        final String loginCallBackHandler = oAuthConfig.getOauthSaslLoginCallbackHandlerClass();
        final String oauthJwksEndpointURL = oAuthConfig.getOauthJwksEndpointURL();
        final String introspectServer = oAuthConfig.getOauthIntrospectServer();


        properties.put(SASL_MECHANISM, saslMechanism);
        properties.put(SECURITY_PROTOCOL, securityProtocol);
        properties.put(SASL_TOKEN_ENDPOINT_URL, tokenEndPointURL);
        properties.put(SASL_CALLBACK_HANDLER_CLASS, loginCallBackHandler);


        if (oauthJwksEndpointURL != null && !oauthJwksEndpointURL.isEmpty() && !oauthJwksEndpointURL.isBlank()) {
            properties.put(SASL_JWKS_ENDPOINT_URL, oauthJwksEndpointURL);
        }

        String instrospect_properties = "";
        if (oauthJwksEndpointURL != null && !oauthIntrospectEndpoint.isBlank() && !oauthIntrospectEndpoint.isEmpty()) {
            instrospect_properties = String.format(INSTROSPECT_SERVER_PROPERTIES, introspectServer, oauthIntrospectEndpoint, oauthAuthorizationToken);
        }

        String jass_config = String.format(OAUTH_JAASCONFIG, oauthClientId, oauthClientSecret, oauthLoginScope, oauthLoginServer,
                oauthLoginEndpoint, oauthLoginGrantType, oauthLoginScope, oauthAuthorizationToken, instrospect_properties);

        if (kafkaClusterAuthConfig instanceof KafkaSourceConfig &&
                "USER_INFO".equalsIgnoreCase(((KafkaSourceConfig) kafkaClusterAuthConfig).getSchemaConfig().getBasicAuthCredentialsSource())) {
            final SchemaConfig schemaConfig = ((KafkaSourceConfig) kafkaClusterAuthConfig).getSchemaConfig();
            final String apiKey = schemaConfig.getSchemaRegistryApiKey();
            final String apiSecret = schemaConfig.getSchemaRegistryApiSecret();
            final String extensionLogicalCluster = oAuthConfig.getExtensionLogicalCluster();
            final String extensionIdentityPoolId = oAuthConfig.getExtensionIdentityPoolId();
            properties.put(REGISTRY_BASIC_AUTH_USER_INFO, apiKey + ":" + apiSecret);
            properties.put("basic.auth.credentials.source", "USER_INFO");
            String extensionValue = "extension_logicalCluster= \"%s\" extension_identityPoolId=  " + " \"%s\";";
            jass_config = jass_config.replace(";", " ");
            jass_config += String.format(extensionValue, extensionLogicalCluster, extensionIdentityPoolId);
        }
        properties.put(SASL_JAAS_CONFIG, jass_config);
    }

    public static void setAwsIamAuthProperties(Properties properties, final AwsIamAuthConfig awsIamAuthConfig, final AwsConfig awsConfig) {
        properties.put(SECURITY_PROTOCOL, "SASL_SSL");
        properties.put(SASL_MECHANISM, "AWS_MSK_IAM");
        properties.put(SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            if (Objects.isNull(awsConfig)) {
                throw new RuntimeException("AWS Config needs to be specified when sasl/aws_msk_iam is set to \"role\"");
            }
            String baseIamAuthConfig = "software.amazon.msk.auth.iam.IAMLoginModule required " +
                "awsRoleArn=\"%s\" " +
                "awsStsRegion=\"%s\"";

            baseIamAuthConfig = String.format(baseIamAuthConfig, awsConfig.getStsRoleArn(), awsConfig.getRegion());

            if (Objects.nonNull(awsConfig.getStsRoleSessionName())) {
                baseIamAuthConfig += String.format(" awsRoleSessionName=\"%s\"", awsConfig.getStsRoleSessionName());
            }

            baseIamAuthConfig += ";";
            properties.put(SASL_JAAS_CONFIG, baseIamAuthConfig);
        } else if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            properties.put(SASL_JAAS_CONFIG,
                    "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
    }

    private static void configureMSKCredentialsProvider(final AuthConfig authConfig, final AwsConfig awsConfig) {
        mskCredentialsProvider = DefaultCredentialsProvider.create();
        if (Objects.nonNull(authConfig) && Objects.nonNull(authConfig.getSaslAuthConfig()) &&
                authConfig.getSaslAuthConfig().getAwsIamAuthConfig() == AwsIamAuthConfig.ROLE) {
            String sessionName = "data-prepper-kafka-session" + UUID.randomUUID();
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(awsConfig.getRegion()))
                    .credentialsProvider(mskCredentialsProvider)
                    .build();
            mskCredentialsProvider = StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                            AssumeRoleRequest
                                    .builder()
                                    .roleArn(awsConfig.getStsRoleArn())
                                    .roleSessionName(sessionName)
                                    .build()
                    ).build();
        }
    }

    public static String getBootStrapServersForMsk(final AwsConfig awsConfig,
                                                   final AwsCredentialsProvider mskCredentialsProvider,
                                                   final Logger log) {
        final AwsConfig.AwsMskConfig awsMskConfig = awsConfig.getAwsMskConfig();
        KafkaClient kafkaClient = KafkaClient.builder()
                .credentialsProvider(mskCredentialsProvider)
                .region(Region.of(awsConfig.getRegion()))
                .build();
        final GetBootstrapBrokersRequest request =
                GetBootstrapBrokersRequest
                        .builder()
                        .clusterArn(awsMskConfig.getArn())
                        .build();

        int numRetries = 0;
        boolean retryable;
        GetBootstrapBrokersResponse result = null;
        do {
            retryable = false;
            try {
                result = kafkaClient.getBootstrapBrokers(request);
            } catch (KafkaException | StsException e) {
                log.info("Failed to get bootstrap server information from MSK. Will try every 10 seconds for {} seconds", 10*MAX_KAFKA_CLIENT_RETRIES, e);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException exp) {}
                retryable = true;
            } catch (Exception e) {
                throw new RuntimeException("Failed to get bootstrap server information from MSK.", e);
            }
        } while (retryable && numRetries++ < MAX_KAFKA_CLIENT_RETRIES);
        if (Objects.isNull(result)) {
            throw new RuntimeException("Failed to get bootstrap server information from MSK after trying multiple times with retryable exceptions.");
        }
        switch (awsMskConfig.getBrokerConnectionType()) {
            case PUBLIC:
                return result.bootstrapBrokerStringPublicSaslIam();
            case MULTI_VPC:
                return result.bootstrapBrokerStringVpcConnectivitySaslIam();
            default:
            case SINGLE_VPC:
                return result.bootstrapBrokerStringSaslIam();
        }
    }
    public static void setDynamicSaslClientCallbackHandler(final Properties properties,
                                                           final KafkaConnectionConfig kafkaConnectionConfig,
                                                           final PluginConfigObservable pluginConfigObservable) {
        final AuthConfig authConfig = kafkaConnectionConfig.getAuthConfig();
        if (Objects.nonNull(authConfig)) {
            AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
            if (Objects.nonNull(saslAuthConfig) && Objects.nonNull(saslAuthConfig.getPlainTextAuthConfig())) {
                final DynamicBasicCredentialsProvider dynamicBasicCredentialsProvider =
                        DynamicBasicCredentialsProvider.getInstance();
                pluginConfigObservable.addPluginConfigObserver(
                        newConfig -> dynamicBasicCredentialsProvider.refresh((KafkaConnectionConfig) newConfig));
                dynamicBasicCredentialsProvider.refresh(kafkaConnectionConfig);
                properties.put(SASL_CLIENT_CALLBACK_HANDLER_CLASS, DynamicSaslClientCallbackHandler.class);
            }
        }
    }
    public static void setAuthProperties(final Properties properties, final KafkaClusterAuthConfig kafkaClusterAuthConfig, final Logger log) {
        final AwsConfig awsConfig = kafkaClusterAuthConfig.getAwsConfig();
        final AuthConfig authConfig = kafkaClusterAuthConfig.getAuthConfig();
        final EncryptionConfig encryptionConfig = kafkaClusterAuthConfig.getEncryptionConfig();
        configureMSKCredentialsProvider(authConfig, awsConfig);

        String bootstrapServers = "";
        if (Objects.nonNull(kafkaClusterAuthConfig.getBootstrapServers())) {
            bootstrapServers = String.join(",", kafkaClusterAuthConfig.getBootstrapServers());
        }
        if (Objects.nonNull(awsConfig) && Objects.nonNull(awsConfig.getAwsMskConfig())) {
            bootstrapServers = getBootStrapServersForMsk(awsConfig, mskCredentialsProvider, log);
        }

        if (Objects.nonNull(authConfig)) {
            final AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
            if (Objects.nonNull(saslAuthConfig)) {
                final AwsIamAuthConfig awsIamAuthConfig = saslAuthConfig.getAwsIamAuthConfig();
                final ScramAuthConfig scramAuthConfig = saslAuthConfig.getScramAuthConfig();
                final PlainTextAuthConfig plainTextAuthConfig = saslAuthConfig.getPlainTextAuthConfig();

                if (Objects.nonNull(awsIamAuthConfig)) {
                    if (checkEncryptionType(encryptionConfig, EncryptionType.NONE)) {
                        throw new RuntimeException("Encryption Config must be SSL to use IAM authentication mechanism");
                    }
                    setAwsIamAuthProperties(properties, awsIamAuthConfig, awsConfig);
                } else if (Objects.nonNull(saslAuthConfig.getOAuthConfig())) {
                    setOauthProperties(kafkaClusterAuthConfig, properties);
                } else if (Objects.nonNull(scramAuthConfig) && Objects.nonNull(kafkaClusterAuthConfig.getEncryptionConfig())) {
                    setScramAuthProperties(properties, scramAuthConfig, kafkaClusterAuthConfig.getEncryptionConfig());
                }  else if (Objects.nonNull(plainTextAuthConfig) && Objects.nonNull(kafkaClusterAuthConfig.getEncryptionConfig())) {
                    setPlainTextAuthProperties(properties, plainTextAuthConfig, kafkaClusterAuthConfig.getEncryptionConfig());
                } else {
                    throw new RuntimeException("No SASL auth config specified");
                }
            }
            if (encryptionConfig.getInsecure()) {
                properties.put(SSL_ENGINE_FACTORY_CLASS, InsecureSslEngineFactory.class);
            }
        }
        if (Objects.isNull(authConfig) || Objects.isNull(authConfig.getSaslAuthConfig())) {
            if (checkEncryptionType(encryptionConfig, EncryptionType.SSL)) {
                properties.put(SECURITY_PROTOCOL, "SSL");
                setSecurityProtocolSSLProperties(properties, encryptionConfig);
            }
        }
        if (Objects.isNull(bootstrapServers) || bootstrapServers.isEmpty()) {
            throw new RuntimeException("Bootstrap servers are not specified");
        }

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    private static boolean checkEncryptionType(final EncryptionConfig encryptionConfig, final EncryptionType encryptionType) {
        return Objects.nonNull(encryptionConfig) && encryptionConfig.getType() == encryptionType;
    }

    public static GlueSchemaRegistryKafkaDeserializer getGlueSerializer(final KafkaConsumerConfig kafkaConsumerConfig) {
        configureAwsGlueCredentialsProvider(kafkaConsumerConfig.getAwsConfig());
        SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig) || schemaConfig.getType() != SchemaRegistryType.AWS_GLUE) {
            return null;
        }
        Map<String, Object> configs = new HashMap<>();
        final AwsConfig awsConfig = kafkaConsumerConfig.getAwsConfig();
        if (Objects.nonNull(awsConfig) && Objects.nonNull(awsConfig.getRegion())) {
            configs.put(AWSSchemaRegistryConstants.AWS_REGION, kafkaConsumerConfig.getAwsConfig().getRegion());
        }
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        configs.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "86400000");
        configs.put(AWSSchemaRegistryConstants.CACHE_SIZE, "10");
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.FULL);
        String endpointOverride = kafkaConsumerConfig.getSchemaConfig().getRegistryURL();
        if (endpointOverride != null) {
            configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, endpointOverride);
        }
        glueDeserializer = new GlueSchemaRegistryKafkaDeserializer(awsGlueCredentialsProvider, configs);
        return glueDeserializer;
    }

    private static void configureAwsGlueCredentialsProvider(final AwsConfig awsConfig) {
        awsGlueCredentialsProvider = DefaultCredentialsProvider.create();
        if (Objects.nonNull(awsConfig) &&
                Objects.nonNull(awsConfig.getRegion()) && Objects.nonNull(awsConfig.getStsRoleArn())) {
            String sessionName = "data-prepper-kafka-session" + UUID.randomUUID();
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(awsConfig.getRegion()))
                    .credentialsProvider(awsGlueCredentialsProvider)
                    .build();
            awsGlueCredentialsProvider = StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                            AssumeRoleRequest
                                    .builder()
                                    .roleArn(awsConfig.getStsRoleArn())
                                    .roleSessionName(sessionName)
                                    .build()
                    ).build();
        }
    }

}

