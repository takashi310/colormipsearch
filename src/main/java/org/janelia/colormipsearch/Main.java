package org.janelia.colormipsearch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static class MainArgs {
        @Parameter(names = "--cacheSize", description = "Max cache size")
        private long cacheSize = 200000L;
        @Parameter(names = "--cacheExpirationInMin", description = "Cache expiration in minutes")
        private long cacheExpirationInMin = 60;
        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    public static void main(String[] argv) {
        MainArgs mainArgs = new MainArgs();
        CommonArgs commonArgs = new CommonArgs();
        ColorDepthSearchJSONInputCmd jsonMIPsSearchCmd = new ColorDepthSearchJSONInputCmd(commonArgs);
        ColorDepthSearchLocalMIPsCmd localMIPFilesSearchCmd = new ColorDepthSearchLocalMIPsCmd(commonArgs);
        CombineResultsCmd combineResultsCmd = new CombineResultsCmd(commonArgs);
        NormalizeGradientScoresCmd fakeGradientScoresCmd = new NormalizeGradientScoresCmd(commonArgs);
        CalculateGradientScoresCmd calculateGradientScoresCmd = new CalculateGradientScoresCmd(commonArgs);
        ReplaceURLsCommand replaceURLsCmd = new ReplaceURLsCommand(commonArgs);

        JCommander cmdline = JCommander.newBuilder()
                .addObject(mainArgs)
                .addCommand("searchFromJSON", jsonMIPsSearchCmd.getArgs())
                .addCommand("searchLocalFiles", localMIPFilesSearchCmd.getArgs())
                .addCommand("combineResults", combineResultsCmd.getArgs())
                .addCommand("gradientScore", calculateGradientScoresCmd.getArgs())
                .addCommand("initGradientScores", fakeGradientScoresCmd.getArgs())
                .addCommand("replaceImageURLs", replaceURLsCmd.getArgs())
                .build();

        try {
            cmdline.parse(argv);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder(e.getMessage()).append('\n');
            if (StringUtils.isNotBlank(cmdline.getParsedCommand())) {
                cmdline.usage(cmdline.getParsedCommand(), sb);
            } else {
                cmdline.usage(sb);
            }
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }

        if (mainArgs.displayHelpMessage) {
            cmdline.usage();
            System.exit(0);
        } else if (commonArgs.displayHelpMessage && StringUtils.isNotBlank(cmdline.getParsedCommand())) {
            cmdline.usage(cmdline.getParsedCommand());
            System.exit(0);
        } else if (StringUtils.isBlank(cmdline.getParsedCommand())) {
            StringBuilder sb = new StringBuilder("Missing command\n");
            cmdline.usage(sb);
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }
        // initialize the cache
        CachedMIPsUtils.initializeCache(mainArgs.cacheSize, mainArgs.cacheExpirationInMin);
        // invoke the appropriate command
        switch (cmdline.getParsedCommand()) {
            case "searchFromJSON":
                CmdUtils.createOutputDirs(jsonMIPsSearchCmd.getArgs().getPerLibraryDir(), jsonMIPsSearchCmd.getArgs().getPerMaskDir());
                jsonMIPsSearchCmd.execute();
                break;
            case "searchLocalFiles":
                CmdUtils.createOutputDirs(localMIPFilesSearchCmd.getArgs().getPerLibraryDir(), localMIPFilesSearchCmd.getArgs().getPerMaskDir());
                localMIPFilesSearchCmd.execute();
                break;
            case "gradientScore":
                if (calculateGradientScoresCmd.getArgs().getResultsDir() == null && calculateGradientScoresCmd.getArgs().getResultsFile() == null) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(cmdline.getParsedCommand(), sb);
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(calculateGradientScoresCmd.getArgs().getOutputDir());
                calculateGradientScoresCmd.execute();
                break;
            case "combineResults":
                if (CollectionUtils.isEmpty(combineResultsCmd.getArgs().resultsDirs) &&
                        CollectionUtils.isEmpty(combineResultsCmd.getArgs().resultsFiles)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(cmdline.getParsedCommand(), sb);
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(combineResultsCmd.getArgs().getOutputDir());
                combineResultsCmd.execute();
                break;
            case "initGradientScores":
                if (CollectionUtils.isEmpty(fakeGradientScoresCmd.getArgs().resultsDirs) &&
                        CollectionUtils.isEmpty(fakeGradientScoresCmd.getArgs().resultsFiles)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(cmdline.getParsedCommand(), sb);
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(fakeGradientScoresCmd.getArgs().getOutputDir());
                fakeGradientScoresCmd.execute();
                break;
            case "replaceImageURLs":
                if (!replaceURLsCmd.getArgs().validate()) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(cmdline.getParsedCommand(), sb);
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(replaceURLsCmd.getArgs().getOutputDir());
                replaceURLsCmd.execute();
                break;
            default:
                StringBuilder sb = new StringBuilder("Invalid command\n");
                cmdline.usage(sb);
                JCommander.getConsole().println(sb.toString());
                System.exit(1);
        }
    }
}
