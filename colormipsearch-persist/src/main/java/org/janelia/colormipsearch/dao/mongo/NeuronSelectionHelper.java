package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.mongodb.client.model.Filters;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;

class NeuronSelectionHelper {

    private static final Document NO_FILTER = new Document();

    static Bson getNeuronFilter(String fieldQualifier, NeuronSelector neuronSelector) {
        if (neuronSelector == null || neuronSelector.isEmpty()) {
            return NO_FILTER;
        }
        String qualifier = StringUtils.isNotBlank(fieldQualifier) ? fieldQualifier + "." : "";

        List<Bson> filter = new ArrayList<>();
        if (neuronSelector.hasNeuronClassname()) {
            filter.add(Filters.eq(qualifier + "class", neuronSelector.getNeuronClassname()));
        }
        if (neuronSelector.hasAlignmentSpace()) {
            filter.add(Filters.eq(qualifier + "alignmentSpace", neuronSelector.getAlignmentSpace()));
        }
        if (neuronSelector.hasLibraries()) {
            filter.add(Filters.in(qualifier + "libraryName", neuronSelector.getLibraries()));
        }
        if (neuronSelector.isCheckIfNameValid()) {
            filter.add(Filters.and(
                    Filters.exists(qualifier + "publishedName"),
                    Filters.ne(qualifier + "publishedName", AbstractNeuronEntity.NO_CONSENSUS))
            );
        }
        if (neuronSelector.hasNames()) {
            filter.add(Filters.in(qualifier + "publishedName", neuronSelector.getNames()));
        }
        if (neuronSelector.hasMipIDs()) {
            filter.add(Filters.in(qualifier + "mipId", neuronSelector.getMipIDs()));
        }
        if (neuronSelector.hasTags()) {
            filter.add(Filters.in(qualifier + "tags", neuronSelector.getTags()));
        }
        if (filter.isEmpty()) {
            return NO_FILTER;
        } else if (filter.size() == 1) {
            return filter.get(0);
        } else {
            return Filters.and(filter);
        }
    }

    static <R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>>
    Bson getNeuronsMatchFilter(NeuronsMatchFilter<R> neuronsMatchFilter) {
        List<Bson> filter = new ArrayList<>();
        if (neuronsMatchFilter != null) {
            addNeuronsMatchScoresFilters(neuronsMatchFilter.getScoresFilter(), filter);
            addInFilter("maskImageRefId", neuronsMatchFilter.getMaskEntityIds(), filter);
            addInFilter("matchedImageRefId", neuronsMatchFilter.getTargetEntityIds(), filter);
            addInFilter("tags", neuronsMatchFilter.getTags(), filter);
        }
        return MongoDaoHelper.createBsonFilterCriteria(filter);
    }

    private static void addNeuronsMatchScoresFilters(ScoresFilter neuronsMatchScoresFilter, List<Bson> filter) {
        if ((neuronsMatchScoresFilter == null || neuronsMatchScoresFilter.isEmpty())) {
            return;
        }
        if (neuronsMatchScoresFilter.hasEntityType()) {
            filter.add(MongoDaoHelper.createFilterByClass(neuronsMatchScoresFilter.getEntityType()));
        }
        neuronsMatchScoresFilter.getScoreSelectors().forEach(s -> filter.add(Filters.gte(s.getFieldName(), s.getMinScore())));
    }

    private static <E> void addInFilter(String attrName, Collection<E> values, List<Bson> filter) {
        if (CollectionUtils.isNotEmpty(values)) {
            filter.add(MongoDaoHelper.createInFilter(attrName, values));
        }
    }
}
