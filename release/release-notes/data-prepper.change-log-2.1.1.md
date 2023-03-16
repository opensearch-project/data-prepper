
* __Use Netty version supplied by dependencies (#2031)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Mar 2023 16:21:11 -0500
    
    EAD -&gt; refs/heads/cl-2.1.1, refs/remotes/upstream/main, refs/remotes/origin/main, refs/remotes/origin/HEAD, refs/heads/main, refs/heads/2.1.1-release-notes
    * Removed old constraints on Netty which were resulting in pulling in older
    version of Netty. Our dependencies (Armeria and AWS SDK Java client) are
    pulling in newer versions so these old configurations are not necessary
    anymore.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix: Fixed IllegalArgumentException in PluginMetrics  (#2369)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 9 Mar 2023 12:54:31 -0600
    
    
    * Fix: Fixed IllegalArgumentException in PluginMetrics caused by pipeline name
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __FIX: traceState not required in Link (#2363)__

    [Qi Chen](mailto:qchea@amazon.com) - Fri, 3 Mar 2023 16:55:37 -0600
    
    
    Signed-off-by: George Chen &lt;qchea@amazon.com&gt;


