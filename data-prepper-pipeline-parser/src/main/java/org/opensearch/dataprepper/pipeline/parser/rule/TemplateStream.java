package org.opensearch.dataprepper.pipeline.parser.rule;

import java.io.IOException;
import java.io.InputStream;

public class TemplateStream {
    private String name;
    private InputStream templateStream;


    public void close() {
        if (templateStream != null) {
            try {
                templateStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
