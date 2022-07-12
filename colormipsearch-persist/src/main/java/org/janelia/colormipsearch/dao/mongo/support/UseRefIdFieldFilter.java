package org.janelia.colormipsearch.dao.mongo.support;

import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;

import org.janelia.colormipsearch.model.annotations.UseRefId;

public class UseRefIdFieldFilter extends SimpleBeanPropertyFilter {
    @Override
    protected boolean include(BeanPropertyWriter writer) {
        return super.include(writer);
    }

    @Override
    protected boolean include(PropertyWriter writer) {
        if (writer.getAnnotation(UseRefId.class) != null) {
            return false;
        } else {
            return true;
        }
    }
}
