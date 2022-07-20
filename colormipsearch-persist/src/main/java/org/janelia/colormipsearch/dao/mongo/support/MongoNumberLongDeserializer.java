package org.janelia.colormipsearch.dao.mongo.support;

import java.io.IOException;
import java.math.BigInteger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.commons.lang3.StringUtils;

public class MongoNumberLongDeserializer extends JsonDeserializer<Long> {

    @Override
    public Long deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        JsonNode node = jsonParser.readValueAsTree();
        if (node.get("$numberLong") != null) {
            return Long.valueOf(node.get("$numberLong").asText());
        } else {
            String nText = node.asText();
            if (StringUtils.isNotBlank(nText)) {
                return Long.valueOf(node.asText());
            } else {
                return null;
            }
        }
    }
}
