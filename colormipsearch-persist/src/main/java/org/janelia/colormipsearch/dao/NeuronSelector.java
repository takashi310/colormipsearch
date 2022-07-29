package org.janelia.colormipsearch.dao;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class NeuronSelector {
    private String neuronClassname; // full class name
    private String libraryName; // library name
    private final List<String> names = new ArrayList<>(); // matching published names
    private final List<String> mipIDs = new ArrayList<>(); // matching MIP IDs
    private final List<Number> entityIds = new ArrayList<>(); // matching internal entity IDs
    private final Set<String> tags = new HashSet<>(); // matching tags

    public String getNeuronClassname() {
        return neuronClassname;
    }

    public NeuronSelector setNeuronClassname(String neuronClassname) {
        this.neuronClassname = neuronClassname;
        return this;
    }

    public boolean hasNeuronClassname() {
        return StringUtils.isNotBlank(neuronClassname);
    }

    public String getLibraryName() {
        return libraryName;
    }

    public NeuronSelector setLibraryName(String libraryName) {
        this.libraryName = libraryName;
        return this;
    }

    public boolean hasLibraryName() {
        return StringUtils.isNotBlank(libraryName);
    }

    public List<String> getNames() {
        return names;
    }

    public NeuronSelector addName(String name) {
        if (StringUtils.isNotBlank(name)) this.names.add(name);
        return this;
    }

    public NeuronSelector addNames(List<String> names) {
        names.forEach(this::addName);
        return this;
    }

    public boolean hasNames() {
        return CollectionUtils.isNotEmpty(names);
    }

    public List<String> getMipIDs() {
        return mipIDs;
    }

    public NeuronSelector addMipID(String mipID) {
        if (StringUtils.isNotBlank(mipID)) this.mipIDs.add(mipID);
        return this;
    }

    public NeuronSelector addMipIDs(List<String> mipIDs) {
        mipIDs.forEach(this::addMipID);
        return this;
    }

    public boolean hasMipIDs() {
        return CollectionUtils.isNotEmpty(mipIDs);
    }

    public List<Number> getEntityIds() {
        return entityIds;
    }

    public NeuronSelector addEntityId(Number entityId) {
        if (entityId != null) this.entityIds.add(entityId);
        return this;
    }

    public NeuronSelector addEntityIds(List<Number> entityIds) {
        entityIds.forEach(this::addEntityId);
        return this;
    }

    public boolean hasEntityIds() {
        return CollectionUtils.isNotEmpty(entityIds);
    }

    public Set<String> getTags() {
        return tags;
    }

    public NeuronSelector addTag(String tag) {
        if (StringUtils.isNotBlank(tag)) this.tags.add(tag);
        return this;
    }

    public NeuronSelector addTags(List<String> tags) {
        tags.forEach(this::addTag);
        return this;
    }

    public boolean hasTags() {
        return CollectionUtils.isNotEmpty(tags);
    }

    public boolean isEmpty() {
        return !hasLibraryName()
                && !hasNames()
                && !hasMipIDs()
                && !hasEntityIds()
                && !hasTags();
    }

}
