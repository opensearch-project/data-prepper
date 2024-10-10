package org.opensearch.dataprepper.plugins.test;

import javax.inject.Named;

@Named
public class TestComponent {
    public String getIdentifier() {
        return "test-component";
    }
}
