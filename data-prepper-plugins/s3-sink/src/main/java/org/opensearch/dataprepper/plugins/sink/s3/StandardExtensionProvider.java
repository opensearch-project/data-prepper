package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

class StandardExtensionProvider implements ExtensionProvider {
    private final String extension;

    static ExtensionProvider create(OutputCodec outputCodec, CompressionOption compressionOption) {

        String codecExtension = outputCodec.getExtension();

        if(outputCodec.isCompressionInternal()) {
            return new StandardExtensionProvider(codecExtension);
        }

        String extension = compressionOption.getExtension()
                .map(compressionExtension -> codecExtension + "." + compressionExtension)
                .orElse(codecExtension);


        return new StandardExtensionProvider(extension);
    }

    private StandardExtensionProvider(String extension) {
        this.extension = extension;
    }

    @Override
    public String getExtension() {
        return extension;
    }
}
