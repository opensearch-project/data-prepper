package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Configuration;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@Configuration
public class JiraAuthFactory implements FactoryBean<JiraAuthConfig> {

    private final JiraSourceConfig sourceConfig;

    public JiraAuthFactory(JiraSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public JiraAuthConfig getObject() {
        String authType = sourceConfig.getAuthType();
        if (OAUTH2.equals(authType)) {
            return new JiraOauthConfig(sourceConfig);
        }
        return new JiraBasicAuthConfig(sourceConfig);
    }

    @Override
    public Class<?> getObjectType() {
        return JiraAuthConfig.class;
    }
}
