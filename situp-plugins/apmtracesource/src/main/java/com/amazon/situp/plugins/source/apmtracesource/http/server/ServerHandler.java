package com.amazon.situp.plugins.source.apmtracesource.http.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpVersion;

import java.net.URI;
import java.net.URISyntaxException;

public class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

  private final String path;
  private final ServerRequestProcessor<FullHttpRequest> requestProcessor;

  public ServerHandler(String path, ServerRequestProcessor<FullHttpRequest> requestProcessor) {
    this.path = path;
    this.requestProcessor = requestProcessor;
  }

  @Override protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
    FullHttpResponse resp;
    if (!(path.equals(parseClientPath(msg)))) {
      resp = ServerResponseUtil.generateResourceNotFound(msg.protocolVersion());
    } else {
      resp = ServerResponseUtil.generateResponse(requestProcessor.processMessage(msg), msg.protocolVersion());
    }
    ctx.writeAndFlush(resp);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.writeAndFlush(ServerResponseUtil.generateResourceInternalError(HttpVersion.HTTP_1_1));
  }

  private String parseClientPath(final FullHttpRequest req) throws URISyntaxException {
    URI uri = new URI(req.uri().toLowerCase());
    return uri.getPath();
  }

}
