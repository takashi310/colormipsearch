package org.janelia.colormipsearch.dao.mongo.support;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

public class RegistryHelper {

    public static CodecRegistry createCodecRegistry() {
        return CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(
                        new BigIntegerCodec()
                ),
                CodecRegistries.fromProviders(new JacksonCodecProvider())
        );
    }

}
