FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml /workspace/pom.xml
COPY nishi-utils-core /workspace/nishi-utils-core
COPY ngrid-test /workspace/ngrid-test

RUN mvn -DskipTests -Dmaven.javadoc.skip=true -Dskip.docker.build=true install
RUN mvn -f ngrid-test/pom.xml -DskipTests -Dskip.docker.build=true package

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/ngrid-test/target/ngrid-test-jar-with-dependencies.jar /app/ngrid-test.jar
COPY --from=build /workspace/ngrid-test/config /app/config

ENTRYPOINT ["java", "-jar", "/app/ngrid-test.jar"]
