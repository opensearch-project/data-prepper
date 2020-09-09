package com.amazon.situp.plugins.processor.state;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class LmdbProcessorStateTest extends ProcessorStateTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    public void setProcessorState() throws Exception {
        this.processorState = new LmdbProcessorState<>(temporaryFolder.newFolder(), "testDb", DataClass.class);
    }
}
