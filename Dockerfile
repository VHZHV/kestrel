FROM eclipse-temurin:17-jre-alpine

WORKDIR /src

EXPOSE 8080
EXPOSE 5005

ENTRYPOINT ./mvnw package -T 1.5C -pl kestrel-example -am && \
           java -jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 /src/kestrel-example/target/kestrel-example-0-SNAPSHOT-jar-with-dependencies.jar
