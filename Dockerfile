FROM azul/zulu-openjdk:21.0.2-21.32-jre

RUN mkdir /neuron-search-tools

WORKDIR /app
COPY target ../neuron-search-tools/target
