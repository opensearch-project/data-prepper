package org.opensearch.dataprepper.parquetInputCodec;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.function.Consumer;

@ExtendWith(MockitoExtension.class)
public class parquetInputCodecTest {

    @Mock
    private Consumer<Record<Event>> eventConsumer;



}
