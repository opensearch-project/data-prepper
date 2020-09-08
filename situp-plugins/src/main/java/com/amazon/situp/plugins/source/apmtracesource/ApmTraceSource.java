package com.amazon.situp.plugins.source.apmtracesource;

import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.source.Source;
import com.amazon.situp.model.PluginType;
import com.amazon.situp.plugins.source.apmtracesource.http.server.NettyHttpConfig;
import com.amazon.situp.plugins.source.apmtracesource.http.server.NettyHttpServer;

@SitupPlugin(name = "apm_trace_source", type = PluginType.SOURCE)
public class ApmTraceSource implements Source<Record<String>> {
  //TODO: Hardcoding the source config to avoid conflicts
  static final NettyHttpConfig DEFAULT_NETTY_HTTP_CONFIG = new NettyHttpConfig(9400,
      "0.0.0.0",
      "/_smart_ingest/traces/v1",
      8,
      1024 * 1024);

  private final NettyHttpConfig nettyHttpConfig;

  private NettyHttpServer nettyHttpServer;

  public ApmTraceSource(final PluginSetting pluginSetting) {
    //TODO: We will default constructor
    this(ApmTraceSource.DEFAULT_NETTY_HTTP_CONFIG);
  }

  public ApmTraceSource(NettyHttpConfig nettyHttpConfig) {
    this.nettyHttpConfig = nettyHttpConfig;
  }

  @Override public void start(Buffer<Record<String>> buffer) {
    if (nettyHttpServer == null) {
      nettyHttpServer = new NettyHttpServer(nettyHttpConfig, new ApmTraceRequestProcessor(buffer));
    }
    nettyHttpServer.startServer();
  }

  @Override public void stop() {
    nettyHttpServer.shutdownServer();
  }
}
