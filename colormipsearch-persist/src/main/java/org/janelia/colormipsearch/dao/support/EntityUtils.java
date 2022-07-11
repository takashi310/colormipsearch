package org.janelia.colormipsearch.dao.support;

import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

public class EntityUtils {

    private static final Map<Class<?>, PersistenceInfo> MONGO_MAPPING_CACHE = new LinkedHashMap<>();

    private static PersistenceInfo loadPersistenceInfo(Class<?> objectClass) {
        PersistenceInfo persistenceInfo = null;
        for(Class<?> clazz = objectClass; clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.isAnnotationPresent(PersistenceInfo.class)) {
                persistenceInfo = clazz.getAnnotation(PersistenceInfo.class);
                break;
            }
        }
        return persistenceInfo;
    }

    public static PersistenceInfo getPersistenceInfo(Class<?> objectClass) {
        if (!MONGO_MAPPING_CACHE.containsKey(objectClass)) {
            MONGO_MAPPING_CACHE.put(objectClass, loadPersistenceInfo(objectClass));
        }
        return MONGO_MAPPING_CACHE.get(objectClass);
    }

}
