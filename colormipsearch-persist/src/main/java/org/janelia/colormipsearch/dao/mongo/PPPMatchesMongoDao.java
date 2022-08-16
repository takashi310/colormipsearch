package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.UnwindOptions;

import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.EntityUtils;
import org.janelia.colormipsearch.dao.IdGenerator;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;

public class PPPMatchesMongoDao<R extends PPPMatchEntity<? extends AbstractNeuronEntity,
                                                         ? extends AbstractNeuronEntity>> extends AbstractNeuronMatchesMongoDao<R> {
    public PPPMatchesMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected List<Bson> createQueryPipeline(Bson matchFilter, NeuronSelector maskImageFilter, NeuronSelector matchedImageFilter) {
        List<Bson> pipeline = new ArrayList<>();

        pipeline.add(Aggregates.match(matchFilter));
        pipeline.add(Aggregates.match(NeuronSelectionHelper.getNeuronFilter("sourceImage", maskImageFilter)));
        pipeline.add(Aggregates.match(NeuronSelectionHelper.getNeuronFilter("targetImage", matchedImageFilter)));

        return pipeline;
    }
}
