package org.opensearch.dataprepper.plugins.codec;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.opensearch.dataprepper.model.codec.CompressionEngine;

import java.io.IOException;
import java.io.OutputStream;

public class ZstdCompressionEngine implements CompressionEngine {
    @Override
    public OutputStream createOutputStream(OutputStream outputStream) throws IOException {
        return new ZstdCompressorOutputStream(outputStream);
    }
}
