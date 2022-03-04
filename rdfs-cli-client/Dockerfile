FROM maven:3.8.4-jdk-11 AS build  
COPY src /usr/src/app/src  
COPY pom.xml /usr/src/app  
RUN mvn -f /usr/src/app/pom.xml clean package

FROM gcr.io/distroless/java  
COPY --from=build /usr/src/app/target/rdfs-cli-client-1.0-SNAPSHOT.jar /usr/app/rdfs-cli-client-1.0-SNAPSHOT.jar  
COPY lib/picocli-4.6.3.jar usr/app/picocli-4.6.3.jar
ENTRYPOINT ["java", "-cp", "usr/app/picocli-4.6.3.jar:usr/app/rdfs-cli-client-1.0-SNAPSHOT.jar", "com.rdfs.CliClient"]
