package org.opensearch.dataprepper.plugins.test;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestExtensionConfig {
    @JsonProperty("test_attribute")
    private String testAttribute;

    public String getTestAttribute() {
        return testAttribute;
    }
}
