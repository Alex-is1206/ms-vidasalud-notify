# Imagen de ms-vidasalud-notify (consumidor de eventos RabbitMQ).
# Construccion en dos etapas: la primera compila con Maven, la segunda
# solo lleva el JRE y el jar, asi la imagen final no arrastra Maven ni el codigo fuente.

# ---------- Etapa 1: compilacion ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Primero solo el pom: si el codigo cambia pero las dependencias no,
# Docker reutiliza esta capa y no vuelve a descargar todo.
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
# -DskipTests compila los tests pero no los ejecuta: si un test no compila, la imagen falla.
RUN mvn -B clean package -DskipTests

# ---------- Etapa 2: ejecucion ----------
FROM eclipse-temurin:21-jre
WORKDIR /app

# Usuario sin privilegios: el proceso no corre como root dentro del contenedor.
RUN groupadd --system vidasalud && useradd --system --gid vidasalud vidasalud

COPY --from=build /app/target/*.jar app.jar
USER vidasalud

EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]