package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

class PrimitiveClassesToTypesArgumentsProvider implements ArgumentsProvider {
    private static final Random RANDOM = new Random();

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        byte[] bytes = new byte[10];
        RANDOM.nextBytes(bytes);
        return Stream.of(
                arguments(UUID.randomUUID().toString(), Schema.Type.STRING),
                arguments(RANDOM.nextInt(10_000), Schema.Type.INT),
                arguments(RANDOM.nextLong(), Schema.Type.LONG),
                arguments(RANDOM.nextFloat(), Schema.Type.FLOAT),
                arguments(RANDOM.nextDouble(), Schema.Type.DOUBLE),
                arguments(RANDOM.nextBoolean(), Schema.Type.BOOLEAN),
                arguments(bytes, Schema.Type.BYTES)
        );
    }
}
