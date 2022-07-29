package org.janelia.colormipsearch.dao.mongo.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.colormipsearch.model.AbstractMatchEntity;

public class RegistryHelper {

    public static CodecRegistry createCodecRegistry() {
        ObjectMapper objectMapper = MongoDBHelper.createMongoObjectMapper();

        return CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(
                        new BigIntegerCodec(),
                        new FileDataCodec(objectMapper)
                ),
                CodecRegistries.fromProviders(
                        new EnumCodecProvider()
                ),
                CodecRegistries.fromProviders(
                        new JacksonCodecProvider(objectMapper)
                )
        );
    }

}
