package org.janelia.colormipsearch.dao.mongo.support;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public class MongoNumberLongDeserializer extends JsonDeserializer<Long> {

    @Override
    public Long deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        JsonNode node = jsonParser.readValueAsTree();
        if (node.get("$numberLong") != null) {
            return Long.valueOf(node.get("$numberLong").asText());
        } else {
            return Long.valueOf(node.asText());
        }
    }
}
