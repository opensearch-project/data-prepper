package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ByteArrayPositionOutputStreamTest {
    @Mock
    private ByteArrayOutputStream innerOutputStream;

    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
    }


    private ByteArrayPositionOutputStream createObjectUnderTest() {
        return new ByteArrayPositionOutputStream(innerOutputStream);
    }

    @Test
    void getPos_returns_size() throws IOException {
        int innerSize = random.nextInt(100_000) + 1_000;
        when(innerOutputStream.size()).thenReturn(innerSize);

        assertThat(createObjectUnderTest().getPos(), equalTo((long) innerSize));
    }
}