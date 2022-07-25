package org.janelia.colormipsearch.dataio.db;

import java.util.List;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.dataio.CDMIPsReader;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBCDMIPsReader implements CDMIPsReader {

    private final NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao;

    public DBCDMIPsReader(Config config) {
        this.neuronMetadataDao = DaosProvider.getInstance(config).getNeuronMetadataDao();
    }

    @Override
    public List<? extends AbstractNeuronMetadata> readMIPs(DataSourceParam inputMipsParam) {
        return  neuronMetadataDao.findNeurons(
                new NeuronSelector().setLibraryName(inputMipsParam.getLocation()),
                new PagedRequest().setPageSize(inputMipsParam.getSize()).setFirstPageOffset(inputMipsParam.getOffset())
        ).getResultList();
    }

}
