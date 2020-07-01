package org.janelia.colormipsearch.cmd;

import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.utils.CachedMIPsUtils;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Main {

    private static class MainArgs {
        @Parameter(names = "--cacheSize", description = "Max cache size")
        private long cacheSize = 200000L;
        @Parameter(names = "--cacheExpirationInSeconds", description = "Cache expiration in seconds")
        private long cacheExpirationInSeconds = 60;
        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    public static void main(String[] argv) {
        MainArgs mainArgs = new MainArgs();
        CommonArgs commonArgs = new CommonArgs();
        GroupMIPsByPublishedNameCmd groupMIPsByPublishedNameCmd = new GroupMIPsByPublishedNameCmd(commonArgs);
        CreateColorDepthSearchJSONInputCmd createColorDepthSearchJSONInputCmd = new CreateColorDepthSearchJSONInputCmd(commonArgs);
        ColorDepthSearchJSONInputCmd jsonMIPsSearchCmd = new ColorDepthSearchJSONInputCmd(commonArgs);
        ColorDepthSearchLocalMIPsCmd localMIPFilesSearchCmd = new ColorDepthSearchLocalMIPsCmd(commonArgs);
        MergeResultsCmd mergeResultsCmd = new MergeResultsCmd(commonArgs);
        NormalizeGradientScoresCmd normalizeGradientScoresCmd = new NormalizeGradientScoresCmd(commonArgs);
        CalculateGradientScoresCmd calculateGradientScoresCmd = new CalculateGradientScoresCmd(commonArgs);
        ReplaceURLsCmd replaceURLsCmd = new ReplaceURLsCmd(commonArgs);
        UpdateGradientScoresFromReverseSearchResultsCmd updateGradientScoresFromReverseSearchResultsCmd = new UpdateGradientScoresFromReverseSearchResultsCmd(commonArgs);

        JCommander cmdline = JCommander.newBuilder()
                .addObject(mainArgs)
                .addCommand("groupMIPsByPublishedName", groupMIPsByPublishedNameCmd.getArgs())
                .addCommand("createColorDepthSearchJSONInput", createColorDepthSearchJSONInputCmd.getArgs())
                .addCommand("searchFromJSON", jsonMIPsSearchCmd.getArgs())
                .addCommand("searchLocalFiles", localMIPFilesSearchCmd.getArgs())
                .addCommand("gradientScore", calculateGradientScoresCmd.getArgs())
                .addCommand("gradientScoresFromMatchedResults", updateGradientScoresFromReverseSearchResultsCmd.getArgs())
                .addCommand("mergeResults", mergeResultsCmd.getArgs())
                .addCommand("normalizeGradientScores", normalizeGradientScoresCmd.getArgs())
                .addCommand("replaceImageURLs", replaceURLsCmd.getArgs())
                .build();

        try {
            cmdline.parse(argv);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder(e.getMessage()).append('\n');
            if (StringUtils.isNotBlank(cmdline.getParsedCommand())) {
                cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
            } else {
                cmdline.getUsageFormatter().usage(sb);
            }
            cmdline.getConsole().println(sb.toString());
            System.exit(1);
        }

        if (mainArgs.displayHelpMessage) {
            cmdline.usage();
            System.exit(0);
        } else if (commonArgs.displayHelpMessage && StringUtils.isNotBlank(cmdline.getParsedCommand())) {
            StringBuilder sb = new StringBuilder();
            cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
            cmdline.getConsole().println(sb.toString());
            System.exit(0);
        } else if (StringUtils.isBlank(cmdline.getParsedCommand())) {
            StringBuilder sb = new StringBuilder("Missing command\n");
            cmdline.getUsageFormatter().usage(sb);
            cmdline.getConsole().println(sb.toString());
            System.exit(1);
        }
        // initialize the cache
        CachedMIPsUtils.initializeCache(mainArgs.cacheSize, mainArgs.cacheExpirationInSeconds);
        // invoke the appropriate command
        switch (cmdline.getParsedCommand()) {
            case "groupMIPsByPublishedName":
                groupMIPsByPublishedNameCmd.execute();
                break;
            case "createColorDepthSearchJSONInput":
                List<String> cdsJSONInputValidationErrors = createColorDepthSearchJSONInputCmd.getArgs().validate();
                if (!cdsJSONInputValidationErrors.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    cdsJSONInputValidationErrors.forEach(err -> sb.append(err).append('\n'));
                    cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
                    cmdline.getConsole().println(sb.toString());
                    System.exit(1);
                }
                createColorDepthSearchJSONInputCmd.execute();
                break;
            case "searchFromJSON":
                CmdUtils.createOutputDirs(jsonMIPsSearchCmd.getArgs().getPerLibraryDir(), jsonMIPsSearchCmd.getArgs().getPerMaskDir());
                jsonMIPsSearchCmd.execute();
                break;
            case "searchLocalFiles":
                CmdUtils.createOutputDirs(localMIPFilesSearchCmd.getArgs().getPerLibraryDir(), localMIPFilesSearchCmd.getArgs().getPerMaskDir());
                localMIPFilesSearchCmd.execute();
                break;
            case "gradientScore":
                List<String> gradScoreArgsValidationErrors = calculateGradientScoresCmd.getArgs().validate();
                if (!gradScoreArgsValidationErrors.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    gradScoreArgsValidationErrors.forEach(err -> sb.append(err).append('\n'));
                    cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
                    cmdline.getConsole().println(sb.toString());
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(calculateGradientScoresCmd.getArgs().getOutputDir());
                calculateGradientScoresCmd.execute();
                break;
            case "gradientScoresFromMatchedResults":
                if (!updateGradientScoresFromReverseSearchResultsCmd.getArgs().validate()) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
                    cmdline.getConsole().println(sb.toString());
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(updateGradientScoresFromReverseSearchResultsCmd.getArgs().getOutputDir());
                updateGradientScoresFromReverseSearchResultsCmd.execute();
                break;
            case "mergeResults":
                if (CollectionUtils.isEmpty(mergeResultsCmd.getArgs().resultsDirs) &&
                        CollectionUtils.isEmpty(mergeResultsCmd.getArgs().resultsFiles)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
                    cmdline.getConsole().println(sb.toString());
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(mergeResultsCmd.getArgs().getOutputDir());
                mergeResultsCmd.execute();
                break;
            case "normalizeGradientScores":
                if (!normalizeGradientScoresCmd.getArgs().validate()) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
                    cmdline.getConsole().println(sb.toString());
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(normalizeGradientScoresCmd.getArgs().getOutputDir());
                normalizeGradientScoresCmd.execute();
                break;
            case "replaceImageURLs":
                if (!replaceURLsCmd.getArgs().validate()) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.getUsageFormatter().usage(cmdline.getParsedCommand(), sb);
                    cmdline.getConsole().println(sb.toString());
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(replaceURLsCmd.getArgs().getOutputDir());
                replaceURLsCmd.execute();
                break;
            default:
                StringBuilder sb = new StringBuilder("Invalid command\n");
                cmdline.getUsageFormatter().usage(sb);
                cmdline.getConsole().println(sb.toString());
                System.exit(1);
        }
    }
}
