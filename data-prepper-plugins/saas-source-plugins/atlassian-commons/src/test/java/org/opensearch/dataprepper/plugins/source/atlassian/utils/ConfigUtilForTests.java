/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.atlassian.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.Oauth2Config;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class ConfigUtilForTests {

    public static final Logger log = LoggerFactory.getLogger(ConfigUtilForTests.class);

    private static InputStream getResourceAsStream(String resourceName) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (inputStream == null) {
            inputStream = ConfigUtilForTests.class.getResourceAsStream("/" + resourceName);
        }
        return inputStream;
    }

    public static AtlassianSourceConfig createJiraConfigurationFromYaml(String fileName) {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        try (InputStream inputStream = getResourceAsStream(fileName)) {
            AtlassianSourceConfig confluenceSourceConfig = objectMapper.readValue(inputStream, AtlassianSourceConfigTest.class);
            Oauth2Config oauth2Config = confluenceSourceConfig.getAuthenticationConfig().getOauth2Config();
            if (oauth2Config != null) {
                ReflectivelySetField.setField(Oauth2Config.class, oauth2Config, "accessToken",
                        new MockPluginConfigVariableImpl("mockAccessToken"));
                ReflectivelySetField.setField(Oauth2Config.class, oauth2Config, "refreshToken",
                        new MockPluginConfigVariableImpl("mockRefreshToken"));
            }
            return confluenceSourceConfig;
        } catch (IOException ex) {
            log.error("Failed to parse pipeline Yaml", ex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
