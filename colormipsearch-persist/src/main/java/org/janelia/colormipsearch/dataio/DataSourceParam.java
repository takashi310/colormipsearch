package org.janelia.colormipsearch.dataio;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DataSourceParam {
    private String location;
    private long offset;
    private int size;

    public DataSourceParam(String location, long offset, int size) {
        this.location = location;
        this.offset = offset;
        this.size = size;
    }

    public DataSourceParam() {
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
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

        return new EqualsBuilder().append(offset, that.offset).append(size, that.size).append(location, that.location).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(location).append(offset).append(size).toHashCode();
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        if (hasOffset() || hasSize()) {
            sbuilder.append(':').append(getOffset());
        }
        if (hasSize()) {
            sbuilder.append(':').append(getSize());
        }
        return location;
    }
}
