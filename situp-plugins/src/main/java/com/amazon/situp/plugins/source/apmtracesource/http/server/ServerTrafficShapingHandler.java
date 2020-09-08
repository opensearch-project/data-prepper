package com.amazon.situp.plugins.source.apmtracesource.http.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class will be used for tracking metrics like connections etc.
 * TBD: Decide on how to integrate with TI.
 */
public class ServerTrafficShapingHandler extends ChannelTrafficShapingHandler {

  private static final AtomicInteger ACTIVE_CONNECTION_COUNT = new AtomicInteger();
  private static final AtomicInteger OVERALL_CONNECTION_COUNT = new AtomicInteger();

  public ServerTrafficShapingHandler() {
    super(0);
  }

  public static AtomicInteger getActiveConnectionCount() {
    return ACTIVE_CONNECTION_COUNT;
  }

  public static AtomicInteger getOverallConnectionCount() {
    return OVERALL_CONNECTION_COUNT;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    ACTIVE_CONNECTION_COUNT.incrementAndGet();
    OVERALL_CONNECTION_COUNT.incrementAndGet();
    super.handlerAdded(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    super.handlerRemoved(ctx);
    ACTIVE_CONNECTION_COUNT.decrementAndGet();
  }
}
