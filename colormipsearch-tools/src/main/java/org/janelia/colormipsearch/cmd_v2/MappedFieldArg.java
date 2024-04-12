package org.janelia.colormipsearch.cmd_v2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.IStringConverter;

import org.apache.commons.lang3.StringUtils;

@Deprecated
class MappedFieldArg {

    static class MappedFieldArgConverter implements IStringConverter<MappedFieldArg> {
        @Override
        public MappedFieldArg convert(String value) {
            List<String> argComponents = Arrays.stream(StringUtils.split(value, ':'))
                    .map(String::trim)
                    .collect(Collectors.toList());
            MappedFieldArg arg = new MappedFieldArg();
            if (argComponents.size() > 0) {
                arg.field = argComponents.get(0);
            }
            if (argComponents.size() > 1 && StringUtils.isNotBlank(argComponents.get(1))) {
                arg.mapping = argComponents.get(1);
            }
            return arg;
        }
    }

    private String field; // source field name
    private String mapping; // target field name

    String getField() {
        return field;
    }

    String getFieldMapping() {
        return StringUtils.isBlank(mapping) ? field : mapping;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(field);
        if (StringUtils.isNotBlank(mapping)) {
            sb.append(':').append(mapping);
        }
        return sb.toString();
    }
}
