FROM openjdk:8-jdk-alpine
VOLUME /tmp
ADD target/TwitterAdapterMicroservice-0.1.0.jar app.jar
ENV JAVA_OPTS=""
ENTRYPOINT exec java $JAVA_OPTS -D java.security.egd=file:/dev/./urandom -jar /app.jar
