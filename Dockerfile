FROM azul/zulu-openjdk:21.0.2-jdk as builder

RUN apt update && \
    apt install -y git

WORKDIR /neuron-search-tools
RUN git clone https://github.com/JaneliaSciComp/colormipsearch.git . && \
    ./mvnw package

FROM azul/zulu-openjdk:21.0.2-jdk

WORKDIR /app
COPY --from=builder /neuron-search-tools/target/colormipsearch-3.1.0-jar-with-dependencies.jar ./colormipsearch-3.1.0-jar-with-dependencies.jar
