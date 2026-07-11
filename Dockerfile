# Build stage
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -Ddir=/app/build

# Run stage
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/build/codecrafters-redis.jar ./redis-server.jar

# Expose Redis port (6379) and Dashboard HTTP port (8080)
EXPOSE 6379
EXPOSE 8080

# Run the server
ENTRYPOINT ["java", "-jar", "redis-server.jar"]
