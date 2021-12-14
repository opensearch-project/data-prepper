## 2021-12-10 Version 1.1.1

---

### Security
* Fixes [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228) by using Apache Log4J 2.15.0. [#718](https://github.com/opensearch-project/data-prepper/pull/718), [#723](https://github.com/opensearch-project/data-prepper/pull/723)
* Run yum update on Docker images to get all security patches at the time of the Data Prepper Docker build. [#717](https://github.com/opensearch-project/data-prepper/pull/717)
