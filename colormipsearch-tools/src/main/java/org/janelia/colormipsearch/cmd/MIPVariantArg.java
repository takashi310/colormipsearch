package org.janelia.colormipsearch.cmd;

import java.util.List;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Splitter;

import org.apache.commons.lang3.StringUtils;

class MIPVariantArg {

    public static class MIPVariantArgValidator implements IValueValidator<MIPVariantArg> {
        @Override
        public void validate(String name, MIPVariantArg value) throws ParameterException {
            if (StringUtils.isBlank(value.libraryName)) {
                throw new ParameterException("Variant library is not specified");
            }
            if (StringUtils.isBlank(value.variantType)) {
                throw new ParameterException("Variant type is not specified");
            }
            if (StringUtils.isBlank(value.variantPath)) {
                throw new ParameterException("Variant path is not specified");
            }
        }
    }

    static class MIPVariantArgConverter implements IStringConverter<MIPVariantArg> {
        @Override
        public MIPVariantArg convert(String value) {
            List<String> argComponents = Splitter.on(":").trimResults().splitToList(value);
            MIPVariantArg arg = new MIPVariantArg();
            if (argComponents.size() > 0) {
                arg.libraryName = argComponents.get(0);
            }
            if (argComponents.size() > 1) {
                arg.variantType = argComponents.get(1);
            }
            if (argComponents.size() > 2 && StringUtils.isNotBlank(argComponents.get(2))) {
                arg.variantPath = argComponents.get(2);
            }
            if (argComponents.size() > 3 && StringUtils.isNotBlank(argComponents.get(3))) {
                arg.variantSuffix = argComponents.get(3);
            }
            return arg;
        }
    }

    String libraryName;
    String variantType;
    String variantPath;
    String variantSuffix;

    @Override
    public String toString() {
        return new StringBuilder()
                .append(libraryName)
                .append(':').append(variantType)
                .append(':').append(variantPath)
                .toString();
    }

}
