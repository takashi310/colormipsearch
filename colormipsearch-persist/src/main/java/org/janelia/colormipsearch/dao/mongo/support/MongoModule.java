package org.janelia.colormipsearch.dao.mongo.support;

import java.util.Date;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

import org.janelia.colormipsearch.model.BaseEntity;

public class MongoModule extends SimpleModule {

    public MongoModule() {
        setNamingStrategy(new MongoNamingStrategy());
        addSerializer(Date.class, new ISODateSerializer());
        addDeserializer(Date.class, new ISODateDeserializer());
        addDeserializer(Long.class, new MongoNumberLongDeserializer());
        addDeserializer(Number.class, new MongoNumberBigIntegerDeserializer());
    }

}
