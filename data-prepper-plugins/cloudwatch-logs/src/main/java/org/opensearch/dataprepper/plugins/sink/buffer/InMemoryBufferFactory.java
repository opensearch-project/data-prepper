package org.opensearch.dataprepper.plugins.sink.buffer;

public class InMemoryBufferFactory implements BufferFactory{
    @Override
    public Buffer getBuffer() {
        return new InMemoryBuffer();
    }
}
