/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsIamAuthConfig;

import java.util.Base64;
import java.util.Properties;

/**
 * * This is static property configure dedicated to authentication related information given in pipeline.yml
 */

public class KafkaSourceSecurityConfigurer {

    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final String SASL_SECURITY_PROTOCOL = "security.protocol";

    private static final String SASL_JAS_CONFIG = "sasl.jaas.config";

    private static final String SASL_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";

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
                properties.put(SASL_SECURITY_PROTOCOL, PLAINTEXT_PROTOCOL);
            } else if (StringUtils.isNotEmpty(saslAuthConfig.getAuthProtocolConfig().getPlaintext()) &&
                    SASL_PLAINTEXT_PROTOCOL.equalsIgnoreCase(saslAuthConfig.getAuthProtocolConfig().getPlaintext())) {
                properties.put(SASL_SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
            }
        }
        properties.put(SASL_JAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
    }*/

    public static void setOauthProperties(final KafkaSourceConfig kafkaSourConfig,
                                          final Properties properties) {
        final OAuthConfig oAuthConfig = kafkaSourConfig.getAuthConfig().getSaslAuthConfig().getOAuthConfig();
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
        properties.put(SASL_SECURITY_PROTOCOL, securityProtocol);
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

        if ("USER_INFO".equalsIgnoreCase(kafkaSourConfig.getSchemaConfig().getBasicAuthCredentialsSource())) {
            final String apiKey = kafkaSourConfig.getSchemaConfig().getSchemaRegistryApiKey();
            final String apiSecret = kafkaSourConfig.getSchemaConfig().getSchemaRegistryApiSecret();
            final String extensionLogicalCluster = oAuthConfig.getExtensionLogicalCluster();
            final String extensionIdentityPoolId = oAuthConfig.getExtensionIdentityPoolId();
            properties.put(REGISTRY_BASIC_AUTH_USER_INFO, apiKey + ":" + apiSecret);
            properties.put("basic.auth.credentials.source", "USER_INFO");
            String extensionValue = "extension_logicalCluster= \"%s\" extension_identityPoolId=  " + " \"%s\";";
            jass_config = jass_config.replace(";", " ");
            jass_config += String.format(extensionValue, extensionLogicalCluster, extensionIdentityPoolId);
        }
        properties.put(SASL_JAS_CONFIG, jass_config);
    }

    public static void setAwsIamAuthProperties(Properties properties, AwsIamAuthConfig awsIamAuthConfig, AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new RuntimeException("AWS Config is not specified");
        }
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "AWS_MSK_IAM");
        properties.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        if (awsIamAuthConfig == AwsIamAuthConfig.ROLE) {
            properties.put("sasl.jaas.config",
                    "software.amazon.msk.auth.iam.IAMLoginModule required " +
                            "awsRoleArn=\"" + awsConfig.getStsRoleArn() +
                            "\" awsStsRegion=\"" + awsConfig.getRegion() + "\";");
        } else if (awsIamAuthConfig == AwsIamAuthConfig.DEFAULT) {
            properties.put("sasl.jaas.config",
                    "software.amazon.msk.auth.iam.IAMLoginModule required;");
        }
    }
}

