package org.janelia.colormipsearch.dao;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class PagedRequest {
    private long firstPageOffset;
    private long pageNumber;
    private int pageSize;
    private List<SortCriteria> sortCriteria;

    public long getFirstPageOffset() {
        return firstPageOffset;
    }

    public void setFirstPageOffset(long firstPageOffset) {
        this.firstPageOffset = firstPageOffset;
    }

    public long getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(long pageNumber) {
        this.pageNumber = pageNumber;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public List<SortCriteria> getSortCriteria() {
        return sortCriteria;
    }

    public void setSortCriteria(List<SortCriteria> sortCriteria) {
        this.sortCriteria = sortCriteria;
    }

    public long getOffset() {
        long offset = 0L;
        if (firstPageOffset > 0) {
            offset = firstPageOffset;
        }
        if (pageNumber > 0 && pageSize > 0) {
            offset += pageNumber * pageSize;
        }
        return offset;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("firstPageOffset", firstPageOffset)
                .append("pageNumber", pageNumber)
                .append("pageSize", pageSize)
                .append("sortCriteria", sortCriteria)
                .build();
    }
}
