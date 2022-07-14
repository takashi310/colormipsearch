package org.janelia.colormipsearch.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class NeuronSelector {
    private String libraryName;
    private List<String> names = new ArrayList<>();
    private List<String> mipIDs = new ArrayList<>();

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
        this.names.add(name);
        return this;
    }

    public NeuronSelector addNames(List<String> names) {
        this.names.addAll(names);
        return this;
    }

    public boolean hasNames() {
        return CollectionUtils.isNotEmpty(names);
    }

    public List<String> getMipIDs() {
        return mipIDs;
    }

    public NeuronSelector addMipID(String mipID) {
        this.mipIDs.add(mipID);
        return this;
    }

    public NeuronSelector addMipIDs(List<String> mipIDs) {
        this.mipIDs.addAll(mipIDs);
        return this;
    }

    public boolean hasMipIDs() {
        return CollectionUtils.isNotEmpty(mipIDs);
    }

    public boolean isEmpty() {
        return !hasLibraryName()
                && !hasNames()
                && !hasMipIDs();
    }

}
