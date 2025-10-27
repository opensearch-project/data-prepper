package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A thread-safe extension of InMemoryBuffer.
 */
public class InMemoryBufferSynchronized extends InMemoryBuffer {

  public InMemoryBufferSynchronized(String batchOptionKeyName) {
    this(batchOptionKeyName, new OutputCodecContext(), null);
  }

  public InMemoryBufferSynchronized(String batchOptionKeyName, OutputCodecContext outputCodecContext, List<String> keys) {
    super(batchOptionKeyName, outputCodecContext, keys);

     this.records = Collections.synchronizedList(new ArrayList<>());
  }

  /**
   * We ensure that writing to the JSON codec is thread-safe.
   * Note that we also need to ensure eventCount increments are atomic.
   */
  @Override
  public synchronized void addRecord(Record<Event> record) {
    super.addRecord(record);
  }
}
