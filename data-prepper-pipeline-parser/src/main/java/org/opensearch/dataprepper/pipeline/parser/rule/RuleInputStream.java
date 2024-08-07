package org.opensearch.dataprepper.pipeline.parser.rule;

import java.io.IOException;
import java.io.InputStream;

public class RuleInputStream {
    private String name;
    private InputStream inputStream;

    public RuleInputStream(String name, InputStream inputStream) {
        this.name = name;
        this.inputStream = inputStream;
    }

    public String getName() {
        return name;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void close() {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

