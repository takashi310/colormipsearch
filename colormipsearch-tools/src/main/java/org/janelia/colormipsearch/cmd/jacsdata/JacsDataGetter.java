package org.janelia.colormipsearch.cmd.jacsdata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cmd.HttpHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacsDataGetter {
    private static final Logger LOG = LoggerFactory.getLogger(JacsDataGetter.class);

    private final String dataServiceURL;
    private final String configURL;
    private final String authorization;
    private final int readBatchSize;

    public JacsDataGetter(String dataServiceURL, String configURL, String authorization, int readBatchSize) {
        this.dataServiceURL = dataServiceURL;
        this.configURL = configURL;
        this.authorization = authorization;
        this.readBatchSize = readBatchSize;
    }

    public Map<String, CDMIPSample> retrieveLMSamplesByName(Set<String> sampleNames) {
        if (CollectionUtils.isEmpty(sampleNames)) {
            return Collections.emptyMap();
        } else {
            return httpRetrieveLMSamplesByName(HttpHelper.createClient(), sampleNames);
        }
    }

    private Map<String, CDMIPSample> httpRetrieveLMSamplesByName(Client httpClient, Set<String> sampleNames) {
        LOG.debug("Read LM metadata for {} samples", sampleNames.size());
        return HttpHelper.retrieveDataStreamForNames(() -> httpClient.target(dataServiceURL)
                                .path("/data/samples")
                                .queryParam("name"),
                        authorization,
                        readBatchSize,
                        sampleNames,
                        new TypeReference<List<CDMIPSample>>() {
                        })
                .filter(sample -> StringUtils.isNotBlank(sample.publishingName))
                .collect(Collectors.toMap(n -> n.name, n -> n));
    }

    public Map<String, CDMIPSample> retrieveLMSamplesByRefs(Set<String> sampleRefs) {
        if (CollectionUtils.isEmpty(sampleRefs)) {
            return Collections.emptyMap();
        } else {
            return httpRetrieveLMSamplesByRefs(HttpHelper.createClient(), sampleRefs);
        }
    }

    private Map<String, CDMIPSample> httpRetrieveLMSamplesByRefs(Client httpClient, Set<String> sampleRefs) {
        LOG.debug("Read LM metadata for {} samples", sampleRefs.size());
        return HttpHelper.retrieveData(httpClient.target(dataServiceURL)
                                .path("/data/samples")
                                .queryParam("refs", sampleRefs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)),
                        authorization,
                        new TypeReference<List<CDMIPSample>>() {
                        })
                .stream()
                .filter(sample -> StringUtils.isNotBlank(sample.publishingName))
                .collect(Collectors.toMap(n -> n.sampleRef, n -> n));
    }

    public Map<String, CDMIPBody> retrieveEMNeuronsByDatasetAndBodyIds(String emDataset,
                                                                       String emDatasetVersion,
                                                                       Set<String> neuronBodyIds) {
        if (CollectionUtils.isEmpty(neuronBodyIds)) {
            return Collections.emptyMap();
        } else {
            return httpRetrieveEMNeuronsByDatasetAndBodyIds(HttpHelper.createClient(), emDataset, emDatasetVersion, neuronBodyIds);
        }
    }

    private Map<String, CDMIPBody> httpRetrieveEMNeuronsByDatasetAndBodyIds(Client httpClient,
                                                                            String emDataset,
                                                                            String emDatasetVersion,
                                                                            Set<String> neuronBodyIds) {
        LOG.debug("Read EM metadata for {} neurons", neuronBodyIds.size());
        return HttpHelper.retrieveDataStreamForNames(() -> httpClient.target(dataServiceURL)
                                .path("/emdata/dataset")
                                .path(emDataset)
                                .path(emDatasetVersion),
                        authorization,
                        readBatchSize,
                        neuronBodyIds,
                        new TypeReference<List<CDMIPBody>>() {
                        })
                .collect(Collectors.toMap(
                        n -> n.name,
                        n -> n));
    }

    public Map<String, CDMIPBody> retrieveEMBodiesByRefs(Set<String> emBodyRefs) {
        if (CollectionUtils.isEmpty(emBodyRefs)) {
            return Collections.emptyMap();
        } else {
            return httpRetrieveEMBodiesByRefs(HttpHelper.createClient(), emBodyRefs);
        }
    }

    private Map<String, CDMIPBody> httpRetrieveEMBodiesByRefs(Client httpClient, Set<String> emBodyRefs) {
        LOG.debug("Read EM metadata for {} EM bodies", emBodyRefs.size());
        return HttpHelper.retrieveData(httpClient.target(dataServiceURL)
                                .path("/emdata/emBodies")
                                .queryParam("refs", emBodyRefs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)),
                        authorization,
                        new TypeReference<List<CDMIPBody>>() {
                        })
                .stream()
                .collect(Collectors.toMap(n -> "EMBody#" + n.id, n -> n));
    }

    public Map<String, ColorDepthMIP> retrieveCDMIPs(Set<String> mipIds) {
        if (CollectionUtils.isEmpty(mipIds)) {
            return Collections.emptyMap();
        } else {
            return httpRetrieveCDMIPs(HttpHelper.createClient(), mipIds);
        }
    }

    private Map<String, ColorDepthMIP> httpRetrieveCDMIPs(Client httpClient, Set<String> mipIds) {
        LOG.debug("Read {} MIPs", mipIds.size());
        return HttpHelper.retrieveData(httpClient.target(dataServiceURL)
                                .path("/data/colorDepthMIPsWithSamples")
                                .queryParam("id", mipIds.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)),
                        authorization,
                        new TypeReference<List<ColorDepthMIP>>() {
                        })
                .stream()
                .collect(Collectors.toMap(n -> n.id, n -> n));
    }

    public Map<String, String> retrieveLibraryNameMapping() {
        Map<String, Object> configJSON = HttpHelper.retrieveData(HttpHelper.createClient().target(configURL)
                                .path("/cdm_library"),
                        null, // this does not require any authorization
                        new TypeReference<Map<String, Object>>() {
                        });
        Object configEntry = configJSON.get("config");
        if (!(configEntry instanceof Map)) {
            LOG.error("Config entry from {} is null or it's not a map", configJSON);
            throw new IllegalStateException("Config entry not found");
        }
        Map<String, String> cdmLibraryNamesMapping = new HashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> configEntryMap = (Map<String, Map<String, Object>>) configEntry;
        configEntryMap.forEach((lid, ldata) -> {
            String lname = (String) ldata.get("name");
            cdmLibraryNamesMapping.put(lid, lname);
        });
        LOG.info("Using {} for mapping library names", cdmLibraryNamesMapping);
        return cdmLibraryNamesMapping;
    }

}
