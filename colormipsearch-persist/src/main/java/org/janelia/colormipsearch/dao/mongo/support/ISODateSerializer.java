package org.janelia.colormipsearch.dao.mongo.support;

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

public class ISODateSerializer extends JsonSerializer<Date> {

    @Override
    public void serialize(Date value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
        } else {
            gen.writeStartObject();
            gen.writeFieldName("$date");
            String isoDate = ISODateTimeFormat.dateTime().print(new DateTime(value));
            gen.writeString(isoDate);
            gen.writeEndObject();
        }
    }
}
