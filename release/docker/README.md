## Docker Build for Data Prepper

### Running a docker build

To build an image, check out the corresponding branch for the version and follow below steps

* From root project, run 

```
./gradlew clean :release:docker:docker -Prelease
```
or 

* if running from the current project 
    
``` 
gradle docker -Prelease
```
    
* The image will be built and will be available for running using docker

### Running built docker image

The built image will be available locally and can be used as any other docker image. 
The root project has some usage examples for reference included in `examples` directory. 
Below is an example command which can also be used to run the built image.

```
docker run \
 --name data-prepper-test \
 --expose 21890 \
 -v /workplace/github/simple-ingest-transformation-utility-pipeline/examples/config/example-pipelines.yml:/usr/share/data-prepper/pipelines.yml \
 -v /workplace/github/simple-ingest-transformation-utility-pipeline/examples/config/example-data-prepper.yml:/usr/share/data-prepper/data-prepper.yml \
 data-prepper/data-prepper:0.8.0
```