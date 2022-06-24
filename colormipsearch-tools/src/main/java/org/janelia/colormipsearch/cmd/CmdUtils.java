package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CmdUtils.class);

    static Executor createCmdExecutor(CommonArgs args) {
        if (args.taskConcurrency > 0) {
            LOG.info("Create a thread pool with {} worker threads ({} available processors for workstealing pool)",
                    args.taskConcurrency, Runtime.getRuntime().availableProcessors());
            return Executors.newFixedThreadPool(
                    args.taskConcurrency,
                    new ThreadFactoryBuilder()
                            .setNameFormat("CMDRUNNER-%d")
                            .setDaemon(true)
                            .build());
        } else {
            LOG.info("Create a workstealing pool with {} worker threads", Runtime.getRuntime().availableProcessors());
            return Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors() - 1);
        }
    }

    static void createDirs(@Nullable Path... dirs) {
        for (Path dir : dirs) {
            if (dir != null) {
                try {
                    // create output directory
                    Files.createDirectories(dir);
                } catch (IOException e) {
                    LOG.error("Error creating directory: {}", dir, e);
                    System.exit(1);
                }
            }
        }
    }

    @Nullable
    static File getOutputFile(Path outputDir, File inputFile) {
        if (outputDir == null) {
            return null;
        } else {
            return outputDir.resolve(inputFile.getName()).toFile();
        }
    }

    @Nullable
    static File getOutputFile(Path outputDir, String fname) {
        if (outputDir == null) {
            return null;
        } else {
            return outputDir.resolve(fname).toFile();
        }
    }

    static List<String> getFilesFromDir(String dirName, int offsetParam, int lengthParam) {
        try {
            int from = Math.max(offsetParam, 0);
            List<String> filenamesList = Files.find(Paths.get(dirName), 1, (p, fa) -> fa.isRegularFile())
                    .skip(from)
                    .map(Path::toString)
                    .collect(Collectors.toList());
            if (lengthParam > 0 && lengthParam < filenamesList.size()) {
                return filenamesList.subList(0, lengthParam);
            } else {
                return filenamesList;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
