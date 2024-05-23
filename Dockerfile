FROM azul/zulu-openjdk:21.0.2-jdk as builder

RUN apt update && \
    apt install -y git

WORKDIR /neuron-search-tools
ENV COMMIT_HASH=57b9dd74
RUN git clone --depth 2 https://github.com/JaneliaSciComp/colormipsearch.git . && \
    git reset --hard ${COMMIT_HASH} && \
    ./mvnw package && \
    echo ${COMMIT_HASH} > .commit

FROM azul/zulu-openjdk:21.0.2-jdk

WORKDIR /app
COPY --from=builder /neuron-search-tools/.commit ./.commit
COPY --from=builder /neuron-search-tools/target/colormipsearch-3.1.0-jar-with-dependencies.jar ./colormipsearch-3.1.0-jar-with-dependencies.jar
