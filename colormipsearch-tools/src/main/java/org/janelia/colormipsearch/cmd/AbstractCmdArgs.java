package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.beust.jcommander.ParametersDelegate;

import org.apache.commons.lang3.StringUtils;

class AbstractCmdArgs {
    @ParametersDelegate
    final CommonArgs commonArgs;

    public AbstractCmdArgs(CommonArgs commonArgs) {
        this.commonArgs = commonArgs;
    }

    Optional<Path> getOutputDirArg() {
        if (StringUtils.isNotBlank(commonArgs.outputDir)) {
            return Optional.of(Paths.get(commonArgs.outputDir));
        } else {
            return Optional.empty();
        }
    }

    Path getOutputDir(String... subDirs) {
        String subPath = Arrays.stream(subDirs)
                .filter(StringUtils::isNotBlank)
                .reduce("", (p1, p2) -> StringUtils.isBlank(p1) ? p2 : p1 + "/" + p2);
        return getOutputDirArg()
                .map(p -> p.resolve(subPath))
                .orElse(null);
    }

    String getConfigFileName() {
        return commonArgs.configFilename;
    }

    List<String> validate() {
        return Collections.emptyList();
    }
}
