FROM registry.cn-beijing.aliyuncs.com/tudb-ai/server:base as ybuild


WORKDIR /opt/

COPY ./ /opt/

RUN mvn  -Dmaven.test.skip=true package

FROM  openjdk:8-slim-bullseye

COPY --from=ybuild  /opt/server/target/server-0.1.0-SNAPSHOT-jar-with-dependencies.jar /opt/

ENV  DATA_PATH=/data/

ENV  PORT=7600

EXPOSE $PORT

WORKDIR /opt/


CMD ["java", "-jar", "server-0.1.0-SNAPSHOT-jar-with-dependencies.jar"]
