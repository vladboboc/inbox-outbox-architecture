# Build context is the repository root, since this module depends on sibling modules.
#   docker compose build order-service
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build

# POMs first so dependency resolution is cached independently of source changes.
COPY pom.xml ./
COPY common-events/pom.xml    common-events/
COPY inbox-support/pom.xml    inbox-support/
COPY order-service/pom.xml    order-service/
COPY shipping-service/pom.xml shipping-service/
RUN --mount=type=cache,target=/root/.m2/repository \
    mvn -B -q -pl order-service -am dependency:go-offline -DskipTests || true

COPY common-events/src common-events/src
COPY order-service/src order-service/src
# maven.test.skip skips test *compilation* too. Tests need Docker-in-Docker for Testcontainers,
# so they belong to `./mvnw verify` on the host, not to the image build.
RUN --mount=type=cache,target=/root/.m2/repository \
    mvn -B -pl order-service -am package -Dmaven.test.skip=true

FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app

# curl is used by the compose healthcheck.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --create-home appuser
USER appuser

COPY --from=build /build/order-service/target/order-service-*.jar /app/app.jar

EXPOSE 8090
# Container-aware heap sizing; virtual threads are enabled via application.yml.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+UseZGC"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
