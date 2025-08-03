FROM eclipse-temurin:21-jdk-jammy AS builder

WORKDIR /app

COPY target/inventory-service-1.0.0-SNAPSHOT.jar app.jar

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]