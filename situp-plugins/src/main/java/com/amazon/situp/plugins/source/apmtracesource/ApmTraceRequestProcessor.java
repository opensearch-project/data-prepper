package com.amazon.situp.plugins.source.apmtracesource;

import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.source.apmtracesource.http.server.ServerRequestProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ApmTraceRequestProcessor implements ServerRequestProcessor<FullHttpRequest> {
  private static Logger LOG = LoggerFactory.getLogger(ApmTraceRequestProcessor.class);

  private static final Charset CHAR_SET = StandardCharsets.UTF_8;
  private final Buffer<Record<String>> buffer;

  public ApmTraceRequestProcessor(Buffer<Record<String>> buffer) {
    this.buffer = buffer;
  }

  @Override
  public HttpResponseStatus processMessage(FullHttpRequest request) {
    final String body = request.content().toString(CHAR_SET);
    try {
      final ArrayList<String> records = ApmSpanProcessor.decodeResourceSpan(body);
      LOG.debug("ApmTraceSource: Processing {} records into buffer",records.size());
      records.stream().map(Record::new).forEach(buffer::write);
    } catch (JsonProcessingException e) {
      return HttpResponseStatus.BAD_REQUEST;
    }
    return HttpResponseStatus.OK;
  }
}
