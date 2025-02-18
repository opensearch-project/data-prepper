package org.opensearch.dataprepper.plugins.source.atlassian.utils;

import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;

public class AtlassianSourceConfigTest extends AtlassianSourceConfig {
    @Override
    public String getOauth2UrlContext() {
        return "test";
    }
}
