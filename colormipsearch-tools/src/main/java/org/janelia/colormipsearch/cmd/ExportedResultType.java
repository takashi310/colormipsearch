package org.janelia.colormipsearch.cmd;

import java.util.Comparator;
import java.util.function.Function;

import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.model.AbstractBaseEntity;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;

public enum ExportedResultType {
    // color depth search matches
    PER_MASK_CDS_MATCHES(CDMatchEntity.class,
                         AbstractNeuronEntity::getMipId, // grouped by MIP ID
                         Comparator.comparingDouble(m -> -(((CDMatchEntity<?,?>) m).getNormalizedScore())),
                         DaosProvider::getCDMatchesDao), // sorted by normalized score descending
    PER_TARGET_CDS_MATCHES(CDMatchEntity.class,
                           AbstractNeuronEntity::getMipId, // grouped by MIP ID
                           Comparator.comparingDouble(m -> -(((CDMatchEntity<?,?>) m).getNormalizedScore())),
                           DaosProvider::getCDMatchesDao), // sorted by normalized score descending
    // PPP matches
    PPP_MATCHES(PPPMatchEntity.class,
                AbstractNeuronEntity::getPublishedName, // grouped by published name
                Comparator.comparingDouble(m -> (((PPPMatchEntity<?,?>) m).getRank())),
                DaosProvider::getPPPMatchesDao), // sorted by rank
    EM_MIPS(EMNeuronEntity.class,
            AbstractNeuronEntity::getPublishedName, // grouped by published name
            null, // don't have to be sorted
            DaosProvider::getCDMatchesDao), // sorted by normalized score descending
    LM_MIPS(LMNeuronEntity.class,
            AbstractNeuronEntity::getPublishedName, // grouped by published name
            null, // don't have to be sorted
            DaosProvider::getCDMatchesDao); // sorted by normalized score descending

    @SuppressWarnings("rawtypes")
    private final Class<? extends AbstractBaseEntity> classType;
    private final Function<AbstractNeuronEntity, String> matchGrouping;
    private final Comparator<AbstractMatchEntity<?, ?>> matchOrdering;
    private final Function<DaosProvider, NeuronMatchesDao<?>> neuronMatchesDao;

    @SuppressWarnings("rawtypes")
    ExportedResultType(Class<? extends AbstractBaseEntity> classsType,
                       Function<AbstractNeuronEntity, String> matchGrouping,
                       Comparator<AbstractMatchEntity<?, ?>> matchOrdering,
                       Function<DaosProvider, NeuronMatchesDao<?>> neuronMatchesDao) {
        this.classType = classsType;
        this.matchGrouping = matchGrouping;
        this.matchOrdering = matchOrdering;
        this.neuronMatchesDao = neuronMatchesDao;
    }

    public String getTypeName() {
        return classType.getName();
    }

    public Function<AbstractNeuronEntity, String> getMatchGrouping() {
        return matchGrouping;
    }

    public Comparator<AbstractMatchEntity<?, ?>> getMatchOrdering() {
        return matchOrdering;
    }

    public Function<DaosProvider, NeuronMatchesDao<?>> getNeuronMatchesDao() {
        return neuronMatchesDao;
    }
}
