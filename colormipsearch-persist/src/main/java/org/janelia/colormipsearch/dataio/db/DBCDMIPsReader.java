package org.janelia.colormipsearch.dataio.db;

import java.util.List;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.dataio.CDMIPsReader;
import org.janelia.colormipsearch.dataio.InputParam;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBCDMIPsReader implements CDMIPsReader {

    private final NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao;

    public DBCDMIPsReader(Config config) {
        this.neuronMetadataDao = DaosProvider.getInstance(config).getNeuronMetadataDao();
    }

    @Override
    public List<? extends AbstractNeuronMetadata> readMIPs(InputParam inputMipsParam) {
        return  neuronMetadataDao.findNeuronMatches(
                new NeuronSelector().setLibraryName(inputMipsParam.getValue()),
                new PagedRequest().setPageSize(inputMipsParam.getSize()).setFirstPageOffset(inputMipsParam.getOffset())
        ).getResultList();
    }

}
