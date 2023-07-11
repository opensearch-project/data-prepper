package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.plugin.kafka.OAuthConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OAuthConfigTest {
    OAuthConfig oAuthConfig;
    private String oauthClientId;
    private String oauthClientSecret;
    private String oauthLoginServer;
    private String oauthLoginEndpoint;
    private String oauthLoginGrantType;
    private String oauthLoginScope;
    private String oauthAuthorizationToken;
    private String oauthIntrospectEndpoint;
    private String tokenEndPointURL;
    private String saslMechanism;
    private String securityProtocol;
    private String loginCallBackHandler;
    private String oauthJwksEndpointURL;

    private static final String YAML_FILE_WITH_CONSUMER_CONFIG = "sample-pipelines.yaml";
    @BeforeEach
    void setUp() throws IOException {
        oAuthConfig = new OAuthConfig();
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
            oAuthConfig = kafkaSourceConfig.getAuthConfig().getSaslAuthConfig().getOAuthConfig();
            oauthClientId= oAuthConfig.getOauthClientId();
            oauthClientSecret = oAuthConfig.getOauthClientSecret();
            oauthLoginServer= oAuthConfig.getOauthLoginServer();
            oauthLoginEndpoint= oAuthConfig.getOauthLoginEndpoint();
            oauthLoginGrantType= oAuthConfig.getOauthLoginGrantType();
            oauthLoginScope= oAuthConfig.getOauthLoginScope();
            oauthAuthorizationToken= Base64.getEncoder().encodeToString((oauthClientId +":" + oauthClientSecret).getBytes());
            oauthIntrospectEndpoint= oAuthConfig.getOauthIntrospectEndpoint();
            tokenEndPointURL= oAuthConfig.getOauthTokenEndpointURL();
            saslMechanism= oAuthConfig.getOauthSaslMechanism();
            securityProtocol = oAuthConfig.getOauthSecurityProtocol();
            loginCallBackHandler= oAuthConfig.getOauthSaslLoginCallbackHandlerClass();
            oauthJwksEndpointURL= oAuthConfig.getOauthJwksEndpointURL();
        }
    }

    @Test
    void testConfig() {
        assertThat(oAuthConfig, notNullValue());
    }
    @Test
    void assertConfigValues(){
        assertEquals(oAuthConfig.getOauthClientId(), "0oa9wc21447Pc5vsV5d7");
        assertEquals(oAuthConfig.getOauthClientSecret(), "aGmOfHqIEvBJGDxXAOOcatiE9PvsPgoEePx8IPPa");
        assertEquals(oAuthConfig.getOauthJwksEndpointURL(), "https://dev-13650048.okta.com/oauth2/default/v1/keys");
        assertEquals(oAuthConfig.getOauthSaslLoginCallbackHandlerClass(),"org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler");
        assertEquals(oAuthConfig.getOauthSaslMechanism(),"OAUTHBEARER");
        //assertEquals(oAuthConfig.getOauthAuthorizationToken(),"");
        assertEquals(oAuthConfig.getOauthIntrospectEndpoint(),"/oauth2/default/v1/introspect");
        assertEquals(oAuthConfig.getOauthIntrospectServer(),"https://dev-13650048.okta.com");
        assertEquals(oAuthConfig.getOauthLoginEndpoint(),"/oauth2/default/v1/token");
        assertEquals(oAuthConfig.getOauthLoginGrantType(),"refresh_token");

    }

    @Test
    @Tag(YAML_FILE_WITH_CONSUMER_CONFIG)
    void assertNotNullForConfigs(){
        /*assertNotNull(oAuthConfig.getOauthClientId());
        assertNotNull(oAuthConfig.getOauthClientSecret());
        assertNotNull(oAuthConfig.getOauthJwksEndpointURL());
        assertNotNull(oAuthConfig.getOauthSaslLoginCallbackHandlerClass());
        assertNotNull(oAuthConfig.getOauthSaslMechanism());
        //assertNotNull(oAuthConfig.getOauthAuthorizationToken());
        //assertNotNull(oAuthConfig.getOauthIntrospectAuthorizationToken());
        assertNotNull(oAuthConfig.getOauthIntrospectEndpoint());
        assertNotNull(oAuthConfig.getOauthIntrospectServer());
        assertNotNull(oAuthConfig.getOauthLoginEndpoint());
        assertNotNull(oAuthConfig.getOauthLoginGrantType());
        assertNotNull(oAuthConfig.getOauthLoginScope());
        assertNotNull(oAuthConfig.getOauthTokenEndpointURL());
        assertNotNull(oAuthConfig.getOauthSecurityProtocol());*/
    }
}
