package org.janelia.colormipsearch.cmd;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.IStringConverter;

import org.apache.commons.lang3.StringUtils;

class NameValueArg {

    static class NameArgConverter implements IStringConverter<NameValueArg> {
        @Override
        public NameValueArg convert(String value) {
            int separator = value.indexOf(':');
            String name;
            String separatedValues;
            if (separator != -1)  {
                name = value.substring(0, separator);
                separatedValues = value.substring(separator + 1);
            } else {
                name = value;
                separatedValues = "";
            }
            if (StringUtils.isBlank(name)) {
                throw new IllegalArgumentException("Name part cannot be blank in " + value);
            }
            List<String> argValues = Arrays.stream(StringUtils.split(separatedValues, ','))
                    .map(String::trim)
                    .collect(Collectors.toList());
            return new NameValueArg(name, argValues);
        }
    }

    private NameValueArg(String argName, List<String> argValues) {
        this.argName = argName;
        this.argValues = argValues;
    }

    String argName;
    List<String> argValues;

    @Override
    public String toString() {
        return argName + ":" + String.join(",", argValues);
    }

}
