package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;

import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataExporter implements DataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataExporter.class);

    final CachedJacsDataHelper jacsDataHelper;
    final DataSourceParam dataSourceParam;
    final Path outputDir;

    protected AbstractDataExporter(CachedJacsDataHelper jacsDataHelper,
                                   DataSourceParam dataSourceParam,
                                   Path outputDir) {
        this.jacsDataHelper = jacsDataHelper;
        this.dataSourceParam = dataSourceParam;
        this.outputDir = outputDir;
    }

    @Override
    public DataSourceParam getDataSource() {
        return dataSourceParam;
    }

    void updateEMNeuron(EMNeuronMetadata emNeuron) {
        ColorDepthMIP mip = jacsDataHelper.getColorDepthMIP(emNeuron.getMipId());
        emNeuron.setLibraryName(jacsDataHelper.getLibraryName(emNeuron.getLibraryName()));
        if (mip != null) {
            mip.updateEMNeuron(emNeuron);
        } else {
            LOG.error("No color depth MIP found for EM MIP {}", emNeuron);
        }
    }

    void updateLMNeuron(LMNeuronMetadata lmNeuron) {
        ColorDepthMIP mip = jacsDataHelper.getColorDepthMIP(lmNeuron.getMipId());
        lmNeuron.setLibraryName(jacsDataHelper.getLibraryName(lmNeuron.getLibraryName()));
        if (mip != null) {
            mip.updateLMNeuron(lmNeuron);
        } else {
            LOG.error("No color depth MIP found for LM MIP {}", lmNeuron);
        }
    }

}
