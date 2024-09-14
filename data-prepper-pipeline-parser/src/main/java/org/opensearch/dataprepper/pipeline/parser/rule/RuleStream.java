package org.opensearch.dataprepper.pipeline.parser.rule;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.InputStream;

@Data
@AllArgsConstructor
public class RuleStream {
    private String name;
    private InputStream ruleStream;


    public void close() {
        if (ruleStream != null) {
            try {
                ruleStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

