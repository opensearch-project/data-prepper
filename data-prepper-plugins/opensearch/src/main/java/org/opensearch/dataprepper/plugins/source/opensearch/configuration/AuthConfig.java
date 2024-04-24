package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.Map;

public class AuthConfig {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String AUTHENTICATION = "authentication";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    private String username;

    private String password;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static AuthConfig readAuthConfig(final PluginSetting pluginSetting) {
        final Map<String, Object> authConfigMap =
                pluginSetting.getTypedMap(AUTHENTICATION, String.class, Object.class);
        return authConfigMap.isEmpty() ? null : OBJECT_MAPPER.convertValue(authConfigMap, AuthConfig.class);
    }
}
