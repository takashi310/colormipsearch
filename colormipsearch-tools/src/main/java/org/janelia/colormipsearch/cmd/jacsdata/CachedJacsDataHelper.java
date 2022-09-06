package org.janelia.colormipsearch.cmd.jacsdata;

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
    private static final Map<String, CDMIPSample> LM_SAMPLES_CACHE = new LinkedHashMap<>();

    private final JacsDataGetter jacsDataGetter;
    private Map<String, String> libraryNameMapping;

    public CachedJacsDataHelper(JacsDataGetter jacsDataGetter) {
        this.jacsDataGetter = jacsDataGetter;
    }

    public void retrieveCDMIPs(Set<String> mipIds) {
        if (CollectionUtils.isNotEmpty(mipIds)) {
            Set<String> toRetrieve = mipIds.stream().filter(mipId -> !CD_MIPS_CACHE.containsKey(mipId)).collect(Collectors.toSet());
            LOG.info("Retrieve {} MIPs to populate missing information", toRetrieve.size());
            CD_MIPS_CACHE.putAll(jacsDataGetter.retrieveCDMIPs(toRetrieve));
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


    public void retrieveLMSamples(Set<String> lmSampleNames) {
        if (CollectionUtils.isNotEmpty(lmSampleNames)) {
            Set<String> toRetrieve = lmSampleNames.stream().filter(n -> !LM_SAMPLES_CACHE.containsKey(n)).collect(Collectors.toSet());
            LOG.info("Retrieve {} samples to populate missing information", toRetrieve.size());
            LM_SAMPLES_CACHE.putAll(jacsDataGetter.retrieveLMSamplesByName(toRetrieve));
        }
    }

    public CDMIPSample getLMSample(String lmName) {
        return LM_SAMPLES_CACHE.get(lmName);
    }

}
