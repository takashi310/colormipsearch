FROM azul/zulu-openjdk:21.0.2-jdk as builder
ARG COMMIT_HASH=8689c27d
ARG TARGETPLATFORM

RUN apt update && \
    apt install -y git

#ENV JAVA_OPTS="-XX:-UseG1GC -XX:+UseZGC"

WORKDIR /neuron-search-tools
RUN git clone --depth 2 https://github.com/JaneliaSciComp/colormipsearch.git . && \
    git reset --hard ${COMMIT_HASH}

RUN ./mvnw package -DskipTests && \
    echo ${COMMIT_HASH} > .commit

FROM azul/zulu-openjdk:21.0.2-jdk
ARG TARGETPLATFORM

WORKDIR /app
COPY --from=builder /neuron-search-tools/.commit ./.commit
COPY --from=builder /neuron-search-tools/target/colormipsearch-3.1.0-jar-with-dependencies.jar ./colormipsearch-3.1.0-jar-with-dependencies.jar
