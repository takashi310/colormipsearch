package org.janelia.colormipsearch.cmd;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.janelia.colormipsearch.cmd.v2dataimport.JSONV2Em2LmMatchesReader;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesWriter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.datarequests.SortDirection;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command is used to export data from the database to the file system in order to upload it to S3.
 */
public class ImportV2CDMatchesCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ImportV2CDMatchesCmd.class);

    @Parameters(commandDescription = "Import v2 color depth matches")
    static class ImportCDMatchesCmdArgs extends AbstractCmdArgs {

        @Parameter(names = {"--results", "-r"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "The location of the v2 results. This can be a list of directories or files ")
        List<ListArg> cdMatches;

        @Parameter(names = {"--processingPartitionSize", "-ps", "--libraryPartitionSize"}, description = "Processing partition size")
        int processingPartitionSize = 100;

        public ImportCDMatchesCmdArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

    }

    private final ImportCDMatchesCmdArgs args;
    private final ObjectMapper mapper;

    public ImportV2CDMatchesCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ImportCDMatchesCmdArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    public ImportCDMatchesCmdArgs getArgs() {
        return args;
    }

    @Override
    public void execute() {
        long startTime = System.currentTimeMillis();

        NeuronMatchesReader<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> cdMatchesReader = getCDMatchesReader();
        NeuronMatchesWriter<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> cdMatchesWriter = getCDSMatchesWriter();

        List<String> cdMatchesLocations = cdMatchesReader.listMatchesLocations(args.cdMatches.stream()
                .map(larg -> new DataSourceParam(
                        null,
                        larg.input,
                        null, // it's not clear from the API but the reader is file based so tags are not important here
                        larg.offset,
                        larg.length))
                .collect(Collectors.toList()));
        int size = cdMatchesLocations.size();
        ItemsHandling.partitionCollection(cdMatchesLocations, args.processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartititionItems -> {
                    long startProcessingPartitionTime = System.currentTimeMillis();
                    // process each item from the current partition sequentially
                    indexedPartititionItems.getValue().forEach(maskIdToProcess -> {
                        // read all matches for the current mask
                        List<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> cdMatchesForMask = getCDMatchesForMask(cdMatchesReader, maskIdToProcess);
                        cdMatchesWriter.write(cdMatchesForMask);
                    });
                    LOG.info("Finished batch {} of {} in {}s - memory usage {}M out of {}M",
                            indexedPartititionItems.getKey(),
                            indexedPartititionItems.getValue().size(),
                            (System.currentTimeMillis() - startProcessingPartitionTime) / 1000.,
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                            (Runtime.getRuntime().totalMemory() / _1M));
                });
        LOG.info("Finished importing {} items in {}s - memory usage {}M out of {}M",
                size,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                (Runtime.getRuntime().totalMemory() / _1M));
    }

    private NeuronMatchesReader<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> getCDMatchesReader() {
        return new JSONV2Em2LmMatchesReader(mapper);
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> NeuronMatchesWriter<CDMatchEntity<M, T>>
    getCDSMatchesWriter() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            // always create new matches
            return new DBNeuronMatchesWriter<>(getDaosProvider().getCDMatchesDao());
        } else {
            throw new IllegalArgumentException("This class should only be used for importing intermediated results into the database");
        }
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity>
    List<CDMatchEntity<M, T>> getCDMatchesForMask(NeuronMatchesReader<CDMatchEntity<M, T>> cdsMatchesReader, String maskCDMipId) {
        LOG.info("Read all color depth matches for {}", maskCDMipId);
        return cdsMatchesReader.readMatchesForMasks(
                null,
                null,
                Collections.singletonList(maskCDMipId),
                null,
                null, // it's not clear from the API but the reader is file based so tags are not important here
                Collections.singletonList(
                        new SortCriteria("normalizedScore", SortDirection.DESC)
                ));
    }

}
