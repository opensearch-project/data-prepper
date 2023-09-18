/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
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

public class KafkaConsumerSecurityConfigurer {

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

    private static AwsCredentialsProvider credentialsProvider;
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

    private static void setPlainTextAuthProperties(Properties properties, final PlainTextAuthConfig plainTextAuthConfig, EncryptionType encryptionType) {
        String username = plainTextAuthConfig.getUsername();
        String password = plainTextAuthConfig.getPassword();
        properties.put(SASL_MECHANISM, "PLAIN");
        properties.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        if (encryptionType == EncryptionType.NONE) {
            properties.put(SECURITY_PROTOCOL, "SASL_PLAINTEXT");
        } else { // EncryptionType.SSL
            properties.put(SECURITY_PROTOCOL, "SASL_SSL");
        }
    }

    public static void setOauthProperties(final KafkaConsumerConfig kafkaConsumerConfig,
                                          final Properties properties) {
        final OAuthConfig oAuthConfig = kafkaConsumerConfig.getAuthConfig().getSaslAuthConfig().getOAuthConfig();
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

        if ("USER_INFO".equalsIgnoreCase(kafkaConsumerConfig.getSchemaConfig().getBasicAuthCredentialsSource())) {
            final String apiKey = kafkaConsumerConfig.getSchemaConfig().getSchemaRegistryApiKey();
            final String apiSecret = kafkaConsumerConfig.getSchemaConfig().getSchemaRegistryApiSecret();
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
            properties.put(SASL_JAAS_CONFIG,
                "software.amazon.msk.auth.iam.IAMLoginModule required " +
                "awsRoleArn=\"" + awsConfig.getStsRoleArn() +
                "\" awsStsRegion=\"" + awsConfig.getRegion() + "\";");
        } else if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            properties.put(SASL_JAAS_CONFIG,
                    "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
    }

    public static String getBootStrapServersForMsk(final AwsIamAuthConfig awsIamAuthConfig, final AwsConfig awsConfig, final Logger LOG) {
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            String sessionName = "data-prepper-kafka-session" + UUID.randomUUID();
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(awsConfig.getRegion()))
                    .credentialsProvider(credentialsProvider)
                    .build();
            credentialsProvider = StsAssumeRoleCredentialsProvider
                    .builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                            AssumeRoleRequest
                                    .builder()
                                    .roleArn(awsConfig.getStsRoleArn())
                                    .roleSessionName(sessionName)
                                    .build()
                    ).build();
        } else if (awsIamAuthConfig != AwsIamAuthConfig.DEFAULT) {
            throw new RuntimeException("Unknown AWS IAM auth mode");
        }
        final AwsConfig.AwsMskConfig awsMskConfig = awsConfig.getAwsMskConfig();
        KafkaClient kafkaClient = KafkaClient.builder()
                .credentialsProvider(credentialsProvider)
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
                LOG.info("Failed to get bootstrap server information from MSK. Will try every 10 seconds for {} seconds", 10*MAX_KAFKA_CLIENT_RETRIES, e);
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

    public static void setAuthProperties(Properties properties, final KafkaConsumerConfig sourceConfig, final Logger LOG) {
        final AwsConfig awsConfig = sourceConfig.getAwsConfig();
        final AuthConfig authConfig = sourceConfig.getAuthConfig();
        final EncryptionConfig encryptionConfig = sourceConfig.getEncryptionConfig();
        final EncryptionType encryptionType = encryptionConfig.getType();

        credentialsProvider = DefaultCredentialsProvider.create();

        String bootstrapServers = sourceConfig.getBootStrapServers().iterator().next();
        AwsIamAuthConfig awsIamAuthConfig = null;
        if (Objects.nonNull(authConfig)) {
            AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
            if (Objects.nonNull(saslAuthConfig)) {
                awsIamAuthConfig = saslAuthConfig.getAwsIamAuthConfig();
                PlainTextAuthConfig plainTextAuthConfig = saslAuthConfig.getPlainTextAuthConfig();

                if (Objects.nonNull(awsIamAuthConfig)) {
                    if (encryptionType == EncryptionType.NONE) {
                        throw new RuntimeException("Encryption Config must be SSL to use IAM authentication mechanism");
                    }
                    if (Objects.isNull(awsConfig)) {
                        throw new RuntimeException("AWS Config is not specified");
                    }
                    setAwsIamAuthProperties(properties, awsIamAuthConfig, awsConfig);
                    bootstrapServers = getBootStrapServersForMsk(awsIamAuthConfig, awsConfig, LOG);
                } else if (Objects.nonNull(saslAuthConfig.getOAuthConfig())) {
                    setOauthProperties(sourceConfig, properties);
                } else if (Objects.nonNull(plainTextAuthConfig)) {
                    setPlainTextAuthProperties(properties, plainTextAuthConfig, encryptionType);
                } else {
                    throw new RuntimeException("No SASL auth config specified");
                }
            }
            if (encryptionConfig.getInsecure()) {
                properties.put("ssl.engine.factory.class", InsecureSslEngineFactory.class);
            }
        }
        if (Objects.isNull(authConfig) || Objects.isNull(authConfig.getSaslAuthConfig())) {
            if (encryptionType == EncryptionType.SSL) {
                properties.put(SECURITY_PROTOCOL, "SSL");
            }
        }
        if (Objects.isNull(bootstrapServers) || bootstrapServers.isEmpty()) {
            throw new RuntimeException("Bootstrap servers are not specified");
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public static GlueSchemaRegistryKafkaDeserializer getGlueSerializer(final KafkaConsumerConfig kafkaConsumerConfig) {
        SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig) || schemaConfig.getType() != SchemaRegistryType.AWS_GLUE) {
            return null;
        }
        Map<String, Object> configs = new HashMap();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, kafkaConsumerConfig.getAwsConfig().getRegion());
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        configs.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "86400000");
        configs.put(AWSSchemaRegistryConstants.CACHE_SIZE, "10");
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.FULL);
        glueDeserializer = new GlueSchemaRegistryKafkaDeserializer(credentialsProvider, configs);
        return glueDeserializer;
    }

}

