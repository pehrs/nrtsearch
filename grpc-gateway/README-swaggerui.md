# Grpc Gateway

# Setup (or update)

The swagger-ui dir is a copy from the dist directory from the [swagger-ui repo](https://github.com/swagger-api/swagger-ui)

Also you should change the line:
```
url: "https://petstore.swagger.io/v2/swagger.json",
```
to
```
url: "/grpc/luceneserver.swagger.json",
```
in the index.html file.

# Usage 

open http://localhost:8088/swaggerui/

# Todo

- Change the Dockerfile and gradle build to download and copy the swagger-ui html/js in place.

