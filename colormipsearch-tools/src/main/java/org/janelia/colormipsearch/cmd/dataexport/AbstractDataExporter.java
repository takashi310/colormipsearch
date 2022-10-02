package org.janelia.colormipsearch.cmd.dataexport;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.model.PublishedURLs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataExporter implements DataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataExporter.class);

    final CachedJacsDataHelper jacsDataHelper;
    final DataSourceParam dataSourceParam;
    private final int relativesUrlsToComponent;
    private final ImageStoreMapping imageStoreMapping;
    final Path outputDir;
    final Executor executor;

    protected AbstractDataExporter(CachedJacsDataHelper jacsDataHelper,
                                   DataSourceParam dataSourceParam,
                                   int relativesUrlsToComponent,
                                   ImageStoreMapping imageStoreMapping,
                                   Path outputDir,
                                   Executor executor) {
        this.jacsDataHelper = jacsDataHelper;
        this.dataSourceParam = dataSourceParam;
        this.relativesUrlsToComponent = relativesUrlsToComponent;
        this.imageStoreMapping = imageStoreMapping;
        this.outputDir = outputDir;
        this.executor = executor;
    }

    @Override
    public DataSourceParam getDataSource() {
        return dataSourceParam;
    }

    void updateEMNeuron(EMNeuronMetadata emNeuron, PublishedURLs publishedURLs) {
        ColorDepthMIP mip = jacsDataHelper.getColorDepthMIP(emNeuron.getMipId());
        // the order matter here because the mapping should be defined on the internal library name
        // so imageStore must be set before the library name was changed
        emNeuron.setImageStore(imageStoreMapping.getImageStore(emNeuron.getLibraryName()));
        emNeuron.setLibraryName(jacsDataHelper.getLibraryName(emNeuron.getLibraryName()));
        if (mip != null) {
            mip.updateEMNeuron(emNeuron, publishedURLs);
        } else {
            LOG.error("No color depth MIP found for EM MIP {}", emNeuron);
        }
    }

    void updateLMNeuron(LMNeuronMetadata lmNeuron, PublishedURLs publishedURLs) {
        ColorDepthMIP mip = jacsDataHelper.getColorDepthMIP(lmNeuron.getMipId());
        // the order matter here because the mapping should be defined on the internal library name
        // so imageStore must be set before the library name was changed
        lmNeuron.setImageStore(imageStoreMapping.getImageStore(lmNeuron.getLibraryName()));
        lmNeuron.setLibraryName(jacsDataHelper.getLibraryName(lmNeuron.getLibraryName()));
        if (mip != null) {
            mip.updateLMNeuron(lmNeuron, publishedURLs);
        } else {
            LOG.error("No color depth MIP found for LM MIP {}", lmNeuron);
        }
    }

    String relativizeURL(String aUrl) {
        if (StringUtils.isBlank(aUrl)) {
            return "";
        } else if (StringUtils.startsWithIgnoreCase(aUrl, "https://") ||
                StringUtils.startsWithIgnoreCase(aUrl, "http://")) {
            if (relativesUrlsToComponent >= 0) {
                URI uri = URI.create(aUrl.replace(' ', '+'));
                Path uriPath = Paths.get(uri.getPath());
                return uriPath.subpath(relativesUrlsToComponent,  uriPath.getNameCount()).toString();
            } else {
                return aUrl;
            }
        } else {
            return aUrl;
        }
    }

}
