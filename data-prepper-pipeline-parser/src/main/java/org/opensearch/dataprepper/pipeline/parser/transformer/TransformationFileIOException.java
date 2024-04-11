package org.opensearch.dataprepper.pipeline.parser.transformer;

import java.io.IOException;

public class TransformationFileIOException extends Throwable {
    public TransformationFileIOException(String format, IOException e) {
    }
}
