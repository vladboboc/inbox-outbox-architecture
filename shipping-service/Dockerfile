# Build context is the repository root, since this module depends on sibling modules.
#   docker compose build shipping-service
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build

COPY pom.xml ./
COPY common-events/pom.xml    common-events/
COPY inbox-support/pom.xml    inbox-support/
COPY order-service/pom.xml    order-service/
COPY shipping-service/pom.xml shipping-service/
RUN --mount=type=cache,target=/root/.m2/repository \
    mvn -B -q -pl shipping-service -am dependency:go-offline -DskipTests || true

COPY common-events/src    common-events/src
COPY inbox-support/src    inbox-support/src
COPY shipping-service/src shipping-service/src
# maven.test.skip skips test *compilation* too. Tests need Docker-in-Docker for Testcontainers,
# so they belong to `./mvnw verify` on the host, not to the image build.
RUN --mount=type=cache,target=/root/.m2/repository \
    mvn -B -pl shipping-service -am package -Dmaven.test.skip=true

FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --create-home appuser
USER appuser

COPY --from=build /build/shipping-service/target/shipping-service-*.jar /app/app.jar

EXPOSE 8091
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+UseZGC"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
