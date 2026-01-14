FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app

# Директории под БД и медиа
RUN mkdir -p /app/data /app/media

# Переменные окружения (пример)
ENV DB_PATH=/app/data/bot.db \
    MEDIA_DIR=/app/media

COPY --from=build /app/target/maxim-test-bot-1.0.0.jar /app/app.jar

VOLUME ["/app/data", "/app/media"]

CMD ["java", "-jar", "/app/app.jar"]