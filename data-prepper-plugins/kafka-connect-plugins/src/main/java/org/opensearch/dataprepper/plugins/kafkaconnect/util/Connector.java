package org.opensearch.dataprepper.plugins.kafkaconnect.util;

import java.util.Map;

public class Connector {
    private final String name;
    private final Map<String, String> config;
    private final Boolean allowReplace;

    public Connector(final String name, final Map<String, String> config, final Boolean allowReplace) {
        this.name = name;
        this.config = config;
        this.allowReplace = allowReplace;
    }

    public String getName() {
        return this.name;
    }

    public Map<String, String> getConfig() {
        config.putIfAbsent("name", name);
        return config;
    }

    public Boolean getAllowReplace() {
        return allowReplace;
    }
}
