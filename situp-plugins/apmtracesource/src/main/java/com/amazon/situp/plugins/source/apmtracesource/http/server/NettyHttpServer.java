package com.amazon.situp.plugins.source.apmtracesource.http.server;

import com.amazon.situp.plugins.source.apmtracesource.http.common.DaemonThreadFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import java.util.concurrent.TimeUnit;

public class NettyHttpServer {
  private static final String HTTP_SERVER_WORKER_THREAD_NAME_PREFIX = "APM-HTTP-SOURCE";
  private static final ByteBufAllocator BYTE_BUF_ALLOCATOR = ByteBufAllocator.DEFAULT;
  private final NettyHttpConfig httpConfig;
  private volatile ServerBootstrap serverBootstrap;
  private NioEventLoopGroup nioEventLoopGroup;

  public NettyHttpServer(final NettyHttpConfig httpConfig, final ServerRequestProcessor<FullHttpRequest> requestProcessor) {
    this.httpConfig = httpConfig;
    nioEventLoopGroup = new NioEventLoopGroup(httpConfig.getWorkerCount(),
        new DaemonThreadFactory(HTTP_SERVER_WORKER_THREAD_NAME_PREFIX));
    serverBootstrap = new ServerBootstrap();
    serverBootstrap
        .group(nioEventLoopGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.ALLOCATOR, BYTE_BUF_ALLOCATOR)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override protected void initChannel(SocketChannel channel) {
            channel.pipeline()
                //TODO: ProtoBufDecoder for OTLP and SSLHandler
                // .addLast(new ServerTrafficShapingHandler())
                .addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(httpConfig.getMaxHttpContentLength()))
                .addLast(new ServerHandler(httpConfig.getPath(), requestProcessor));
          }
        });
  }

  public void startServer() {
    try {
      final ChannelFuture channel = serverBootstrap.bind(httpConfig.getHost(), httpConfig.getPort());
      System.out.println(String.format("Starting netty server on %s:%s", httpConfig.getHost(), httpConfig.getPort()));
      channel.sync().channel().closeFuture().sync();
    } catch (final InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  public void shutdownServer() {
    try {
      nioEventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS).sync();
    } catch (final InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

}
