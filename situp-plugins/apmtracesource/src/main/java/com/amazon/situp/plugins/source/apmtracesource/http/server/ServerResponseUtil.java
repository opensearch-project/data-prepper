package com.amazon.situp.plugins.source.apmtracesource.http.server;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class ServerResponseUtil {

  public static FullHttpResponse generateResourceNotFound(final HttpVersion httpVersion) {
    final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, HttpResponseStatus.NOT_FOUND);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
    return response;
  }

  public static FullHttpResponse generateResourceInternalError(final HttpVersion httpVersion) {
    final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, HttpResponseStatus.NOT_FOUND);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
    return response;
  }

  public static FullHttpResponse generateResponse(final HttpResponseStatus httpResponseStatus, final HttpVersion httpVersion) {
    final FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpResponseStatus);
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
    return response;
  }
}
