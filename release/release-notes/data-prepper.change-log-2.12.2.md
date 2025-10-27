
* __Generated THIRD-PARTY file for 4d49f4c (#6174)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 14 Oct 2025 15:12:15 -0700
    
    EAD -&gt; refs/heads/2.12, tag: refs/tags/2.12.2, refs/remotes/upstream/2.12
    Signed-off-by: GitHub &lt;noreply@github.com&gt; Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Require full TLS trust in OpenSearch plugins by default unless insecure is configured (#6165) (#6171)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Oct 2025 14:59:58 -0700
    
    
    Require full TLS trust in OpenSearch plugins by default unless insecure is
    configured. Update the integration tests and end-to-end tests to set the
    insecure flag.
    
    (cherry picked from commit 98fcf0d0ff9c18f1f7501e11dbed918814724b99)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Signed-off-by: Jeremy Michael
    &lt;jsusanto@amazon.com&gt;

* __Updates the JDK version to 17.0.16 from 17.0.10. This is the latest Temurin binary for JDK 17. (#6170) (#6172)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 14 Oct 2025 14:20:08 -0700
    
    
    (cherry picked from commit ae5e701bc4167c2578445e4b0a2cbce48309a395)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Use standard TLS when downloading the database from an HTTP URL. (#6163) (#6167)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 14 Oct 2025 13:25:04 -0700
    
    
    (cherry picked from commit b0386a5af3fb71094ba6c86cd8b2afc783246599)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Change &#34;SSL&#34; to &#34;TLS&#34; (#6164) (#6166)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 14 Oct 2025 12:56:37 -0700
    
    
    (cherry picked from commit db11ce8f27ebca018980b2bca863f7173de9ce56)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Updates to Data Prepper 2.12.2 (#6162)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Oct 2025 10:01:14 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates BouncyCastle to 1.81, fixing CVE-2025-8916. (#6000) (#6001)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 17 Sep 2025 12:20:30 -0700
    
    efs/remotes/ghsa-43ff-rr26-8hx4/2.12
    (cherry picked from commit ea11b9685879b84c51d2072096b27a40c86831aa)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Require the smoke tests to pass before attempting to promote now that they work well. Update the RELEASING.md documentation on this. (#5997) (#6086)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 15 Sep 2025 13:38:09 -0700
    
    
    (cherry picked from commit d75ab26383c2ab10bb7e25c01a45316de002f4a7)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Resolves CVEs by updating Netty to 4.1.125 and commons-lang to 3.18.0. Fixes CVE-2025-58057, CVE-2025-58056, and CVE-2025-48924. (#6081) (#6085)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 15 Sep 2025 13:37:19 -0700
    
    
    (cherry picked from commit b5a0dec857a0287684e8b8588d7978119e91760d)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Update Netty to 4.1.124 to fix CVE-2025-55163. (#5996) (#5998)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 18 Aug 2025 09:10:47 -0700
    
    
    (cherry picked from commit bf40fb81c44fb09edc7a3ee3f8b0a6e7631c64bd)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Changelog for Data Prepper 2.12.1 (#5957) (#5963)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 5 Aug 2025 17:01:39 -0700
    
    
    (cherry picked from commit 7f1f2bd2285dd09a675f1f9065513c582b31c2f7)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Release notes for Data Prepper 2.12.1 (#5955) (#5964)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 5 Aug 2025 17:01:32 -0700
    
    
    (cherry picked from commit b8ffadb96dc69d4ba864a52edcf4f7625976239a)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;


