package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cmd.dataexport.DataExporter;
import org.janelia.colormipsearch.cmd.dataexport.MIPsExporter;
import org.janelia.colormipsearch.cmd.dataexport.EMPPPMatchesExporter;
import org.janelia.colormipsearch.cmd.dataexport.EMCDMatchesExporter;
import org.janelia.colormipsearch.cmd.dataexport.LMCDMatchesExporter;
import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.cmd.jacsdata.JacsDataGetter;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command is used to export data from the database to the file system in order to upload it to S3.
 */
class ExportData4NBCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ExportData4NBCmd.class);

    @Parameters(commandDescription = "Export neuron matches")
    static class ExportMatchesCmdArgs extends AbstractCmdArgs {

        @Parameter(names = {"--exported-result-type"}, required = true, // this is required because PPPs are handled a bit differently
                description = "Specifies neuron match type whether it's color depth search, PPP, etc.")
        ExportedResultType exportedResultType = ExportedResultType.EM_CD_MATCHES;

        @Parameter(names = {"--jacs-url", "--data-url"}, description = "JACS data service base URL")
        String dataServiceURL;

        @Parameter(names = {"--config-url"}, description = "Config URL that contains the library name mapping")
        String configURL = "http://config.int.janelia.org/config";

        @Parameter(names = {"--authorization"}, description = "JACS authorization - this is the value of the authorization header")
        String authorization;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels = 0.0;

        @Parameter(names = {"--ignore-grad-scores"},
                description = "Ignore gradient scores when selecting color depth matches",
                arity = 0)
        boolean ignoreGradScores = false;

        @Parameter(names = {"-as", "--alignment-space"}, description = "Alignment space")
        String alignmentSpace;

        @Parameter(names = {"-l", "--library"}, description = "Selected library for the mask or target based on the export type",
                converter = ListArg.ListArgConverter.class)
        ListArg library;

        @Parameter(names = {"--exported-names"}, description = "If set only export the specified names", variableArity = true)
        List<String> exportedNames;

        @Parameter(names = {"--subdir"}, description = "Results subdirectory for results")
        String subDir;

        @Parameter(names = {"--processingPartitionSize", "-ps", "--libraryPartitionSize"}, description = "Processing partition size")
        int processingPartitionSize = 5000;

        @Parameter(names = {"--tags"}, description = "Tags to be exported", variableArity = true)
        List<String> tags = new ArrayList<>();

        ExportMatchesCmdArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        @Nullable
        Path getOutputResultsDir() {
            return getOutputDirArg()
                    .map(dir -> StringUtils.isNotBlank(subDir) ? dir.resolve(subDir) : dir)
                    .orElse(null);
        }
    }

    private final ExportMatchesCmdArgs args;
    private final ObjectMapper mapper;

    ExportData4NBCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ExportMatchesCmdArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    ExportMatchesCmdArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        exportNeuronMatches();
    }

    private void exportNeuronMatches() {
        DataExporter dataExporter = getDataExporter();
        dataExporter.runExport();
    }

    private ScoresFilter getCDScoresFilter() {
        ScoresFilter neuronsMatchScoresFilter = new ScoresFilter();
        neuronsMatchScoresFilter.setEntityType(CDMatchEntity.class.getName());
        if (args.pctPositivePixels > 0) {
            neuronsMatchScoresFilter.addSScore("matchingPixelsRatio", args.pctPositivePixels / 100);
        }
        if (!args.ignoreGradScores) {
            neuronsMatchScoresFilter.addSScore("gradientAreaGap", 0);
        }
        return neuronsMatchScoresFilter;
    }

    private ScoresFilter getPPPScoresFilter() {
        ScoresFilter neuronsMatchScoresFilter = new ScoresFilter();
        neuronsMatchScoresFilter.setEntityType(PPPMatchEntity.class.getName());
        neuronsMatchScoresFilter.addSScore("rank", 0);
        return neuronsMatchScoresFilter;
    }

    private DataExporter getDataExporter() {
        DaosProvider daosProvider = getDaosProvider();
        ItemsWriterToJSONFile itemsWriter = new ItemsWriterToJSONFile(
                args.commonArgs.noPrettyPrint
                        ? mapper.writer()
                        : mapper.writerWithDefaultPrettyPrinter());
        CachedJacsDataHelper jacsDataHelper = new CachedJacsDataHelper(
                new JacsDataGetter(args.dataServiceURL, args.configURL, args.authorization, args.processingPartitionSize)
        );
        DataSourceParam dataSource = new DataSourceParam(args.alignmentSpace,
                args.library.input,
                args.tags,
                args.library.offset,
                args.library.length)
                .setNames(args.exportedNames);
        switch (args.exportedResultType) {
            case EM_CD_MATCHES:
                return new EMCDMatchesExporter(
                        jacsDataHelper,
                        dataSource,
                        getCDScoresFilter(),
                        args.getOutputResultsDir(),
                        new DBNeuronMatchesReader<>(
                                daosProvider.getNeuronMetadataDao(),
                                daosProvider.getCDMatchesDao()
                        ),
                        itemsWriter,
                        args.processingPartitionSize
                );
            case LM_CD_MATCHES:
                return new LMCDMatchesExporter(
                        jacsDataHelper,
                        dataSource,
                        getCDScoresFilter(),
                        args.getOutputResultsDir(),
                        new DBNeuronMatchesReader<>(
                                daosProvider.getNeuronMetadataDao(),
                                daosProvider.getCDMatchesDao()
                        ),
                        itemsWriter,
                        args.processingPartitionSize
                );
            case EM_PPP_MATCHES:
                return new EMPPPMatchesExporter(
                        jacsDataHelper,
                        dataSource,
                        getPPPScoresFilter(),
                        args.getOutputResultsDir(),
                        new DBNeuronMatchesReader<>(
                                daosProvider.getNeuronMetadataDao(),
                                daosProvider.getPPPMatchesDao()
                        ),
                        itemsWriter,
                        args.processingPartitionSize
                );
            case EM_MIPS:
                return new MIPsExporter(
                        jacsDataHelper,
                        dataSource,
                        args.getOutputResultsDir(),
                        daosProvider.getNeuronMetadataDao(),
                        itemsWriter,
                        args.processingPartitionSize,
                        EMNeuronMetadata.class
                );
            case LM_MIPS:
                return new MIPsExporter(
                        jacsDataHelper,
                        dataSource,
                        args.getOutputResultsDir(),
                        daosProvider.getNeuronMetadataDao(),
                        itemsWriter,
                        args.processingPartitionSize,
                        LMNeuronMetadata.class
                );
            default:
                throw new IllegalArgumentException("Invalid export result type");
        }
    }

}
