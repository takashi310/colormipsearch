package org.janelia.colormipsearch.dao;

import java.util.List;

public interface IdGenerator {
    Number generateId();
    List<Number> generateIdList(long n);
}
