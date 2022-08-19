package org.janelia.colormipsearch.cmd.jacsdata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedJacsDataHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CachedJacsDataHelper.class);

    private static final Map<String, ColorDepthMIP> CD_MIPS_CACHE = new LinkedHashMap<>();
    private final JacsDataGetter jacsDataGetter;
    private Map<String, String> libraryNameMapping;

    public CachedJacsDataHelper(JacsDataGetter jacsDataGetter) {
        this.jacsDataGetter = jacsDataGetter;
    }

    public Map<String, ColorDepthMIP> retrieveCDMIPs(Set<String> mipIds) {
        if (CollectionUtils.isEmpty(mipIds)) {
            return Collections.emptyMap();
        } else {
            Set<String> toRetrieve = mipIds.stream().filter(mipId -> !CD_MIPS_CACHE.containsKey(mipId)).collect(Collectors.toSet());
            CD_MIPS_CACHE.putAll(jacsDataGetter.retrieveCDMIPs(toRetrieve));
            return mipIds.stream().filter(CD_MIPS_CACHE::containsKey)
                    .collect(Collectors.toMap(mId -> mId, CD_MIPS_CACHE::get));
        }
    }

    public ColorDepthMIP getColorDepthMIP(String mipId) {
        return CD_MIPS_CACHE.get(mipId);
    }

    public String getLibraryName(String libname) {
        if (libraryNameMapping == null) {
            libraryNameMapping = jacsDataGetter.retrieveLibraryNameMapping();
        }
        return libraryNameMapping.getOrDefault(libname, libname);
    }
}
