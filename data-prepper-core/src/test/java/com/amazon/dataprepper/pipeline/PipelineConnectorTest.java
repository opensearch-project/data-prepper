package com.amazon.dataprepper.pipeline;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class PipelineConnectorTest {
    private static final String RECORD_DATA = "RECORD_DATA";
    private static final Record<String> RECORD = new Record<>(RECORD_DATA);
    private static final String SINK_PIPELINE_NAME = "SINK_PIPELINE_NAME";

    @Mock
    private Buffer<Record<String>> buffer;

    private List<Record<String>> recordList;

    private PipelineConnector<Record<String>> sut;

    @Before
    public void setup() {
        recordList = Collections.singletonList(RECORD);

        sut = new PipelineConnector<>();
    }

    @Test(expected = RuntimeException.class)
    public void testOutputWithoutBufferInitialized() {
        sut.output(recordList);
    }

    @Test(expected = RuntimeException.class)
    public void testOutputAfterBufferStopRequested() {
        sut.start(buffer);
        sut.stop();

        sut.output(recordList);
    }

    @Test
    public void testOutputBufferTimesOutThenSucceeds() throws Exception {
        doThrow(new TimeoutException()).doNothing().when(buffer).write(any(), anyInt());

        sut.start(buffer);

        sut.output(recordList);

        verify(buffer, times(2)).write(eq(RECORD), anyInt());
    }

    @Test
    public void testOutputSuccess() throws Exception {
        sut.start(buffer);

        sut.output(recordList);

        verify(buffer).write(eq(RECORD), anyInt());
    }

    @Test
    public void testSetSinkPipelineName() {
        sut.setSinkPipelineName(SINK_PIPELINE_NAME);

        try {
            sut.output(recordList);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SINK_PIPELINE_NAME));
        }
    }
}
