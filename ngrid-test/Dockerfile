FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml /workspace/pom.xml
COPY src /workspace/src
COPY ngrid-test /workspace/ngrid-test

RUN mvn -q -DskipTests install
RUN mvn -q -f ngrid-test/pom.xml -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar /app/ngrid-test.jar
COPY --from=build /workspace/ngrid-test/config /app/config

ENTRYPOINT ["java", "-jar", "/app/ngrid-test.jar"]
