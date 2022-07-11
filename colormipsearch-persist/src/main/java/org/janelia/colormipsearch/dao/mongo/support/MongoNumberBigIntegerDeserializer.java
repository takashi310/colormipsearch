package org.janelia.colormipsearch.dao.mongo.support;

import java.io.IOException;
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public class MongoNumberBigIntegerDeserializer extends JsonDeserializer<Number> {

    @Override
    public Number deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        JsonNode node = jsonParser.readValueAsTree();
        if (node.get("$numberLong") != null) {
            return new BigInteger(node.get("$numberLong").asText());
        } else {
            return new BigInteger(node.asText());
        }
    }
}
