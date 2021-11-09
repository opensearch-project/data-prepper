# Log Analytics

## Introduction

Data Prepper is an extendable, configurable, and scalable solution for log ingestion into OpenSearch and Amazon OpenSearch Service. Currently, Data Prepper is focused on receiving logs from [FluentBit](https://fluentbit.io/) via the 
[http source](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/http-source/README.md), and processing those logs with a [grok prepper](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/grok-prepper/README.md) before ingesting them into OpenSearch through the [OpenSearch sink](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/opensearch/README.md).

Here is all of the components for log analytics with FluentBit, Data Prepper, and OpenSearch:
<br />
<br />
![Log Analytics Pipeline](images/LogAnalyticsComponents.png)
<br />
<br />

In your application environment you will have to run FluentBit.
FluentBit can be containerized through Kubernetes, Docker, or Amazon ECS.
It can also be run as an agent on EC2.
You should configure the [FluentBit http output plugin]() to export log data to Data Prepper.
You will then have to deploy Data Prepper as an intermediate component and configure it to send
the enriched log data to your OpenSearch cluster or Amazon OpenSeaarch Service domain. From there, you can
use OpenSearch Dashboards to perform more intensive visualization and analysis.

## Sources


## Next Steps

Follow the [Log Ingestion Demo Guide]() to get a specific example of apache log ingestion from `FluentBit -> Data Prepper -> OpenSearch` running through Docker.

In the future, Data Prepper will contain additional sources and preppers which will make more complex log analytic pipelines available. Check out our [Roadmap]() to see what is coming.  

If there is a specifc source, prepper, or sink that you would like to include in your log analytic workflow, and it is not currently on the Roadmap, please bring it to our attention by making a Github issue. Additionally, if you
are interested in contributing, see our [Contribuing Guidelines]().