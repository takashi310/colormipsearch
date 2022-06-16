package org.janelia.colormipsearch.cmd;

import java.util.Collections;
import java.util.List;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Splitter;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;

class LibraryVariantArg {

    public static class LibraryVariantArgValidator implements IValueValidator<LibraryVariantArg> {
        @Override
        public void validate(String name, LibraryVariantArg value) throws ParameterException {
            if (StringUtils.isBlank(value.libraryName)) {
                throw new ParameterException("Library name is not specified");
            }
            if (value.variantType == null) {
                throw new ParameterException("Library variant type is not specified for " + value);
            }
            if (StringUtils.isBlank(value.variantPath)) {
                throw new ParameterException("Library variant path is not specified for " + value);
            }
        }
    }

    public static class ListLibraryVariantArgValidator implements IValueValidator<List<LibraryVariantArg>> {
        private final LibraryVariantArgValidator singleValueValidator = new LibraryVariantArgValidator();

        @Override
        public void validate(String name, List<LibraryVariantArg> listValue) throws ParameterException {
            for (LibraryVariantArg value: listValue) {
                singleValueValidator.validate(name, value);
            }
        }
    }

    static class LibraryVariantArgConverter implements IStringConverter<LibraryVariantArg> {
        @Override
        public LibraryVariantArg convert(String value) {
            List<String> argComponents = StringUtils.isBlank(value)
                ? Collections.emptyList()
                : Splitter.on(':')
                    .trimResults()
                    .splitToList(value);
            LibraryVariantArg arg = new LibraryVariantArg();
            if (argComponents.size() > 0) {
                arg.libraryName = argComponents.get(0);
            }
            if (argComponents.size() > 1 && StringUtils.isNotBlank(argComponents.get(1))) {
                arg.variantType = argComponents.get(1);
            }
            if (argComponents.size() > 2 && StringUtils.isNotBlank(argComponents.get(2))) {
                arg.variantPath = argComponents.get(2);
            }
            if (argComponents.size() > 3 && StringUtils.isNotBlank(argComponents.get(3))) {
                arg.variantTypeSuffix = argComponents.get(3);
            }
            if (argComponents.size() > 4 && StringUtils.isNotBlank(argComponents.get(4))) {
                arg.variantNameSuffix = argComponents.get(4);
            }
            return arg;
        }
    }

    String libraryName;
    String variantType;
    String variantPath;
    String variantTypeSuffix; // this is typically the suffix appended to a foldername such as _gradient or _RGB20x
    String variantNameSuffix; // this is a suffix that ends the filename itself such as _FL

    @Override
    public String toString() {
        return new StringBuilder()
                .append(libraryName)
                .append(':').append(variantType)
                .append(':').append(variantPath)
                .toString();
    }

}
