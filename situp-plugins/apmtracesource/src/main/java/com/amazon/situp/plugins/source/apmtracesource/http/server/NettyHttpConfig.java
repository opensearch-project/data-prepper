package com.amazon.situp.plugins.source.apmtracesource.http.server;

public class NettyHttpConfig {
  private final int port;
  private final String host;
  private final String path;
  private final int workerCount;
  private final int maxHttpContentLength;

  public NettyHttpConfig(int port, String host, String path, int workerCount, int maxHttpContentLength) {
    this.port = port;
    this.host = host;
    this.path = path;
    this.workerCount = workerCount;
    this.maxHttpContentLength = maxHttpContentLength;
  }

  public int getPort() {
    return port;
  }

  public String getHost() {
    return host;
  }

  public String getPath() {
    return path;
  }

  public int getWorkerCount() {
    return workerCount;
  }

  public int getMaxHttpContentLength() {
    return maxHttpContentLength;
  }
}
