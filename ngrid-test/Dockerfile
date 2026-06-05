FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml /workspace/pom.xml
COPY nishi-utils-core /workspace/nishi-utils-core
COPY nishi-utils-oss /workspace/nishi-utils-oss
COPY ngrid-test /workspace/ngrid-test

# jacoco.skip: o gate de cobertura é responsabilidade do `mvn verify` no CI, não
# do empacotamento da imagem (que roda com -DskipTests e portanto sem dados).
RUN mvn -pl nishi-utils-core -am -DskipTests -Djacoco.skip=true -Dmaven.javadoc.skip=true -Dskip.docker.build=true install
RUN mvn -f ngrid-test/pom.xml -DskipTests -Djacoco.skip=true -Dskip.docker.build=true package

FROM eclipse-temurin:21-jre
WORKDIR /app

# iproute2 fornece o `tc` usado pelo entrypoint para aplicar netem (latencia)
# de forma opcional via NETEM_DELAY — necessario para o IT de mesh concorrente.
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/ngrid-test/target/ngrid-test-jar-with-dependencies.jar /app/ngrid-test.jar
COPY --from=build /workspace/ngrid-test/config /app/config
COPY ngrid-test/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
