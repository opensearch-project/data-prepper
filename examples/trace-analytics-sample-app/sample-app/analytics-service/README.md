# Analytics Service
This directory contains a RESTful Spring Boot Application running on port 8087.

The app contains two endpoints:
- /logs
- /metrics

##### Commands to build app and run the JAR:

Building and running this project requires JDK 17 or above.

```
$ ./gradlew clean build
```
```
$ java -jar build/libs/analytics-service-0.0.1-SNAPSHOT.jar
```

##### Running the app using Docker:
The Dockerfile contains a DEPENDENCY param which unpacks the fat jar in build/dependency. This is for faster performance.
```
$ docker build -t analytics-service .
```
Run the image:
```
$ docker run -p 8087:8087 -t analytics-service
```
##### Alternatively, from the root project directory, this service can be run alongside other services using docker-compose
```
$ cd analytics-service
$ ./gradlew clean build
```
```
$ docker-compose up --build
```

#### Sample requests

To verify the application is running, you can provide some sample requests.

```
curl http://localhost:8087/metrics
```

```
curl http://localhost:8087/logs -X POST -H 'Content-Type: application/json' -d '{"service":"analytics", "message":"my message"}'
```
