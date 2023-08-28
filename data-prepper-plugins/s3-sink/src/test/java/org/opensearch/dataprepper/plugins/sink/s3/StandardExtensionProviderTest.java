package org.opensearch.dataprepper.plugins.sink.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardExtensionProviderTest {

    @Mock
    private OutputCodec outputCodec;

    @Mock
    private CompressionOption compressionOption;

    private String codecExtension;

    @BeforeEach
    void setUp() {
        codecExtension = UUID.randomUUID().toString();
    }

    @Test
    void getExtension_returns_extension_of_codec_when_compression_internal() {
        when(outputCodec.getExtension()).thenReturn(codecExtension);
        when(outputCodec.isCompressionInternal()).thenReturn(true);

        ExtensionProvider extensionProvider = StandardExtensionProvider.create(outputCodec, compressionOption);
        assertThat(extensionProvider, notNullValue());
        assertThat(extensionProvider.getExtension(), equalTo(codecExtension));

        verify(compressionOption, never()).getExtension();
    }

    @Test
    void getExtension_returns_extension_of_codec_compression_has_no_extension() {
        when(outputCodec.getExtension()).thenReturn(codecExtension);
        when(compressionOption.getExtension()).thenReturn(Optional.empty());

        ExtensionProvider extensionProvider = StandardExtensionProvider.create(outputCodec, compressionOption);
        assertThat(extensionProvider, notNullValue());
        assertThat(extensionProvider.getExtension(), equalTo(codecExtension));

        verify(compressionOption).getExtension();
    }

    @Test
    void getExtension_returns_extension_of_codec_compression_has_extension() {
        String compressionExtension = UUID.randomUUID().toString();
        when(outputCodec.getExtension()).thenReturn(codecExtension);
        when(compressionOption.getExtension()).thenReturn(Optional.of(compressionExtension));

        ExtensionProvider extensionProvider = StandardExtensionProvider.create(outputCodec, compressionOption);
        assertThat(extensionProvider, notNullValue());
        assertThat(extensionProvider.getExtension(), equalTo(codecExtension + "." + compressionExtension));
    }

}