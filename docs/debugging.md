# Debugging Data Prepper

This page serves as a guide to debugging Data Prepper.
You can enable Java debugging by using `JAVA_OPTS`.


## Docker

The following Docker compose snippet shows you how to set up debugging for Docker compose.

```yaml
services:
  data-prepper:
    container_name: data-prepper
    image: opensearchproject/data-prepper:2
    ports:
      - "5005:5005"
      # other ports you need
    environment:
      - JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
    # More configuration
```
