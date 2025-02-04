/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.confluence.utils.MockPluginConfigVariableImpl;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;

public class ConfluenceSourceConfigTest {
    private ConfluenceSourceConfig confluenceSourceConfig;
    private final PluginConfigVariable accessToken = new MockPluginConfigVariableImpl("access token test");
    private final PluginConfigVariable refreshToken = new MockPluginConfigVariableImpl("refresh token test");
    private final String clientId = "client id test";
    private final String clientSecret = "client secret test";
    private final String password = "test Jira Credential";
    private final String username = "test Jira Id";
    private final String accountUrl = "https://example.atlassian.net";
    private final List<String> spacesList = new ArrayList<>();
    private final List<String> contentTypeList = new ArrayList<>();

    private ConfluenceSourceConfig createConfluenceSourceConfig(String authtype, boolean hasToken) throws Exception {
        PluginConfigVariable pcvAccessToken = null;
        PluginConfigVariable pcvRefreshToken = null;
        Map<String, Object> configMap = new HashMap<>();
        List<String> hosts = new ArrayList<>();
        hosts.add(accountUrl);

        configMap.put("hosts", hosts);

        Map<String, Object> authenticationMap = new HashMap<>();
        Map<String, String> basicMap = new HashMap<>();
        Map<String, Object> oauth2Map = new HashMap<>();
        if (authtype.equals(BASIC)) {
            basicMap.put("username", username);
            basicMap.put("password", password);
            authenticationMap.put("basic", basicMap);
        } else if (authtype.equals(OAUTH2)) {
            if (hasToken) {
                pcvRefreshToken = refreshToken;
                pcvAccessToken = accessToken;
            } else {
                oauth2Map.put("refresh_token", null);
            }
            oauth2Map.put("client_id", clientId);
            oauth2Map.put("client_secret", clientSecret);
            authenticationMap.put("oauth2", oauth2Map);
        }

        configMap.put("authentication", authenticationMap);

        spacesList.add("space1");
        spacesList.add("space2");

        contentTypeList.add("page");
        contentTypeList.add("blogpost");

        Map<String, Object> filterMap = new HashMap<>();
        Map<String, Object> projectMap = new HashMap<>();
        Map<String, Object> issueTypeMap = new HashMap<>();
        Map<String, Object> statusMap = new HashMap<>();

        issueTypeMap.put("include", contentTypeList);
        filterMap.put("page_type", issueTypeMap);


        Map<String, Object> nameMap = new HashMap<>();
        nameMap.put("include", spacesList);
        projectMap.put("key", nameMap);
        filterMap.put("space", projectMap);

        configMap.put("filter", filterMap);

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonConfig = objectMapper.writeValueAsString(configMap);
        ConfluenceSourceConfig config = objectMapper.readValue(jsonConfig, ConfluenceSourceConfig.class);
        if (config.getAuthenticationConfig().getOauth2Config() != null && pcvAccessToken != null) {
            ReflectivelySetField.setField(Oauth2Config.class,
                    config.getAuthenticationConfig().getOauth2Config(), "accessToken", pcvAccessToken);
            ReflectivelySetField.setField(Oauth2Config.class,
                    config.getAuthenticationConfig().getOauth2Config(), "refreshToken", pcvRefreshToken);
        }
        return config;
    }

    @Test
    void testGetters() throws Exception {
        confluenceSourceConfig = createConfluenceSourceConfig(BASIC, false);
        assertEquals(confluenceSourceConfig.getFilterConfig().getPageTypeConfig().getInclude(), contentTypeList);
        assertEquals(confluenceSourceConfig.getFilterConfig().getSpaceConfig().getNameConfig().getInclude(), spacesList);
        assertEquals(confluenceSourceConfig.getAccountUrl(), accountUrl);
        assertEquals(confluenceSourceConfig.getAuthenticationConfig().getBasicConfig().getPassword(), password);
        assertEquals(confluenceSourceConfig.getAuthenticationConfig().getBasicConfig().getUsername(), username);
    }

    @Test
    void testFetchGivenOauthAttributeWrongAuthType() throws Exception {
        confluenceSourceConfig = createConfluenceSourceConfig(BASIC, true);
        assertThrows(RuntimeException.class, () -> confluenceSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken());
    }

    @Test
    void testFetchGivenOauthAtrribute() throws Exception {
        confluenceSourceConfig = createConfluenceSourceConfig(OAUTH2, true);
        assertEquals(accessToken, confluenceSourceConfig.getAuthenticationConfig().getOauth2Config().getAccessToken());
        assertEquals(refreshToken, confluenceSourceConfig.getAuthenticationConfig().getOauth2Config().getRefreshToken());
        assertEquals(clientId, confluenceSourceConfig.getAuthenticationConfig().getOauth2Config().getClientId());
        assertEquals(clientSecret, confluenceSourceConfig.getAuthenticationConfig().getOauth2Config().getClientSecret());
    }
}
