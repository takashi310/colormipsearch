package org.janelia.colormipsearch.dao.support;

import java.util.List;

public interface IdGenerator {
    Number generateId();
    List<Number> generateIdList(long n);
}
