package org.opensearch.dataprepper.model.codec;

import java.io.IOException;
import java.io.InputStream;

public interface DecompressionEngine {
    InputStream createInputStream(final InputStream responseInputStream) throws IOException;
}
