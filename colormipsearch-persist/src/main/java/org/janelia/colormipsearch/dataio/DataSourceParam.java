package org.janelia.colormipsearch.dataio;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DataSourceParam {
    private String alignmentSpace;
    private String libraryName;
    private Collection<String> mipIDs;
    private Collection<String> names;
    private Collection<String> tags;
    private long offset;
    private int size;

    public DataSourceParam(String alignmentSpace, String libraryName, List<String> tags, long offset, int size) {
        this.alignmentSpace = alignmentSpace;
        this.libraryName = libraryName;
        this.tags = tags;
        this.offset = offset;
        this.size = size;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public DataSourceParam setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
        return this;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public DataSourceParam setLibraryName(String libraryName) {
        this.libraryName = libraryName;
        return this;
    }

    public Collection<String> getNames() {
        return names;
    }

    public DataSourceParam setNames(Collection<String> names) {
        this.names = names;
        return this;
    }

    public Collection<String> getMipIDs() {
        return mipIDs;
    }

    public DataSourceParam setMipIDs(Collection<String> mipIDs) {
        this.mipIDs = mipIDs;
        return this;
    }

    public Collection<String> getTags() {
        return tags;
    }

    public DataSourceParam setTags(Collection<String> tags) {
        this.tags = tags;
        return this;
    }

    public long getOffset() {
        return offset > 0 ? offset : 0;
    }

    public DataSourceParam setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public boolean hasOffset() {
        return offset > 0;
    }

    public int getSize() {
        return size;
    }

    public DataSourceParam setSize(int size) {
        this.size = size;
        return this;
    }

    public boolean hasSize() {
        return size > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        DataSourceParam that = (DataSourceParam) o;

        return new EqualsBuilder()
                .append(alignmentSpace, that.alignmentSpace)
                .append(libraryName, that.libraryName)
                .append(offset, that.offset)
                .append(size, that.size)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(alignmentSpace)
                .append(libraryName)
                .append(offset)
                .append(size)
                .toHashCode();
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        if (StringUtils.isNotBlank(alignmentSpace)) {
            sbuilder.append(alignmentSpace).append('/');
        }
        sbuilder.append(libraryName);
        if (hasOffset() || hasSize()) {
            sbuilder.append(':').append(getOffset());
        }
        if (hasSize()) {
            sbuilder.append(':').append(getSize());
        }
        return sbuilder.toString();
    }
}
