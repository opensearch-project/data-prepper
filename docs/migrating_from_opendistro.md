# Migrating from Open Distro Data Prepper

Starting with Data Prepper 1.1, there is only one distribution of
Data Prepper - Open Search Data Prepper. This document is here to help existing users migrate
from the old Open Distro Data Prepper to OpenSearch Data Prepper.


### Change your Pipeline Configuration

The `elasticsearch` sink has changed to `opensearch`. You will
need to change your existing pipeline to use the `opensearch` plugin
instead of `elasticsearch`.

Please note that while the plugin is titled `opensearch` it remains compatible
with Open Distro and ElasticSearch 7.x.

### Update Docker Image

The Open Distro Data Prepper Docker image was located at `amazon/opendistro-for-elasticsearch-data-prepper`.
You will need to change this value to `opensearchproject/opensearch-data-prepper`.

## More Information

You can find more information about Data Prepper configurations
by going to the [Getting Started](getting_started.md) guide.
