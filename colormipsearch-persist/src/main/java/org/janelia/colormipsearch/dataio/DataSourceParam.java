package org.janelia.colormipsearch.dataio;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DataSourceParam {
    private String alignmentSpace;
    private String libraryName;
    private long offset;
    private int size;

    public DataSourceParam(String alignmentSpace, String libraryName, long offset, int size) {
        this.alignmentSpace = alignmentSpace;
        this.libraryName = libraryName;
        this.offset = offset;
        this.size = size;
    }

    public DataSourceParam() {
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public long getOffset() {
        return offset > 0 ? offset : 0;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean hasOffset() {
        return offset > 0;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
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
