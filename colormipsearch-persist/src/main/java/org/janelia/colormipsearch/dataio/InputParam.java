package org.janelia.colormipsearch.dataio;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class InputParam {
    private String value;
    private long offset;
    private int size;

    public InputParam(String value, long offset, int size) {
        this.value = value;
        this.offset = offset;
        this.size = size;
    }

    public InputParam() {
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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

        InputParam that = (InputParam) o;

        return new EqualsBuilder().append(offset, that.offset).append(size, that.size).append(value, that.value).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(value).append(offset).append(size).toHashCode();
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
        return value;
    }
}
