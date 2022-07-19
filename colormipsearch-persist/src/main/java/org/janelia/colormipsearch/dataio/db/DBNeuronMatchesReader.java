package org.janelia.colormipsearch.dataio.db;

import java.util.List;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.InputParam;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBNeuronMatchesReader<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> implements NeuronMatchesReader<M, T, R> {

    private final NeuronMetadataDao<M> neuronMetadataDao;
    private final NeuronMatchesDao<M, T, R> neuronMatchesDao;

    public DBNeuronMatchesReader(Config config) {
        this.neuronMetadataDao = DaosProvider.getInstance(config).getNeuronMetadataDao();
        this.neuronMatchesDao = DaosProvider.getInstance(config).getNeuronMatchesDao();
    }

    @Override
    public List<String> listMatchesLocations(List<InputParam> cdMatchInputs) {
        return cdMatchInputs.stream()
                        .flatMap(cdMatchInput -> neuronMetadataDao.findNeuronMatches(
                                new NeuronSelector().setLibraryName(cdMatchInput.getValue()),
                                new PagedRequest()
                                        .setFirstPageOffset(cdMatchInput.getOffset())
                                        .setPageSize(cdMatchInput.getSize())
                        ).getResultList().stream().map(AbstractNeuronMetadata::getId))
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<R> readMatches(String maskSource) {
        return neuronMatchesDao.findNeuronMatches(
                        new NeuronSelector().addMipID(maskSource),
                        new NeuronSelector(),
                        new PagedRequest()
                ).getResultList();
    }
}
