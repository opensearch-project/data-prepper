package org.opensearch.dataprepper.plugins.codec;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;

import java.io.IOException;
import java.io.InputStream;

public class ZstdDecompressionEngine implements DecompressionEngine {
    @Override
    public InputStream createInputStream(InputStream responseInputStream) throws IOException {
        return new ZstdCompressorInputStream(responseInputStream);
    }
}
