package org.janelia.colormipsearch.cmd.jacsdata;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cmd.HttpHelper;
import org.janelia.colormipsearch.dao.PublishedImageDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacsDataGetter {
    private static final Logger LOG = LoggerFactory.getLogger(JacsDataGetter.class);

    private final PublishedImageDao publishedImageDao;
    private final String dataServiceURL;
    private final String configURL;
    private final String authorization;
    private final int readBatchSize;

    public JacsDataGetter(PublishedImageDao publishedImageDao,
                          String dataServiceURL,
                          String configURL,
                          String authorization,
                          int readBatchSize) {
        this.publishedImageDao = publishedImageDao;
        this.dataServiceURL = dataServiceURL;
        this.configURL = configURL;
        this.authorization = authorization;
        this.readBatchSize = readBatchSize;
    }

    public Map<String, CDMIPSample> retrieveLMSamplesByName(Set<String> sampleNames) {
        if (CollectionUtils.isEmpty(sampleNames)) {
            return Collections.emptyMap();
        } else {
            return CDMIPSample.indexBySampleName(
                    httpRetrieveLMSamplesByName(HttpHelper.createClient(), sampleNames));
        }
    }

    private List<CDMIPSample> httpRetrieveLMSamplesByName(Client httpClient, Set<String> sampleNames) {
        if (CollectionUtils.isEmpty(sampleNames)) {
            return Collections.emptyList();
        } else {
            LOG.debug("Read LM metadata for {} samples", sampleNames.size());
            return HttpHelper.retrieveDataStreamForNames(() -> httpClient.target(dataServiceURL)
                                    .path("/data/samples"),
                            authorization,
                            readBatchSize,
                            sampleNames,
                            new TypeReference<List<CDMIPSample>>() {
                            })
                    .collect(Collectors.toList());
        }
    }

    private List<CDMIPSample> httpRetrieveLMSamplesByRefs(Client httpClient, Set<String> sampleRefs) {
        LOG.debug("Read LM metadata for {} samples", sampleRefs.size());
        if (CollectionUtils.isEmpty(sampleRefs)) {
            return Collections.emptyList();
        }
        return HttpHelper.retrieveData(httpClient.target(dataServiceURL)
                        .path("/data/samples")
                        .queryParam("refs", sampleRefs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)),
                authorization,
                new TypeReference<List<CDMIPSample>>() {
                },
                Collections.emptyList());
    }

//    private void update3DStack(Client httpClient, ColorDepthMIP colorDepthMIP) {
//        if (colorDepthMIP.sample != null) {
//            updateLM3DImageStack(httpClient, colorDepthMIP);
//        } else if (colorDepthMIP.emBody != null && colorDepthMIP.emBody.files != null) {
//            colorDepthMIP.emSWCFile = colorDepthMIP.emBody.files.get("SkeletonSWC");
//        }
//    }

//    private void updateLM3DImageStack(Client httpClient, ColorDepthMIP colorDepthMIP) {
//        LOG.debug("Read LM 3D stack {}", colorDepthMIP);
//        Map<String, SamplePublishedData> publishedImages = HttpHelper.retrieveData(httpClient.target(dataServiceURL)
//                        .path("/publishedImage/imageWithGen1Image")
//                        .path(colorDepthMIP.alignmentSpace)
//                        .path(colorDepthMIP.sample.slideCode)
//                        .path(colorDepthMIP.objective),
//                authorization,
//                new TypeReference<Map<String, SamplePublishedData>>() {
//                },
//                Collections.emptyMap());
//        SamplePublishedData sample3DImage = publishedImages.get("VisuallyLosslessStack");
//        SamplePublishedData gen1Gal4ExpressionImage = publishedImages.get("SignalMipExpression");
//        colorDepthMIP.sample3DImageStack = sample3DImage != null ? sample3DImage.files.get("VisuallyLosslessStack") : null;
//        colorDepthMIP.sampleGen1Gal4ExpressionImage = gen1Gal4ExpressionImage != null ? gen1Gal4ExpressionImage.files.get("ColorDepthMip1") : null;
//    }

    private List<CDMIPBody> httpRetrieveEMNeuronsByDatasetAndBodyIds(Client httpClient,
                                                                     String emDataset,
                                                                     String emDatasetVersion,
                                                                     Set<String> neuronBodyIds) {
        if (CollectionUtils.isEmpty(neuronBodyIds)) {
            return Collections.emptyList();
        } else {
            LOG.debug("Read EM metadata for {} neurons", neuronBodyIds.size());
            return HttpHelper.retrieveDataStreamForNames(() -> httpClient.target(dataServiceURL)
                            .path("/emdata/dataset")
                            .path(emDataset)
                            .path(emDatasetVersion),
                    authorization,
                    readBatchSize,
                    neuronBodyIds,
                    new TypeReference<List<CDMIPBody>>() {
                    }).collect(Collectors.toList());
        }
    }

    private List<CDMIPBody> httpRetrieveEMBodiesByRefs(Client httpClient, Set<String> emBodyRefs) {
        if (CollectionUtils.isEmpty(emBodyRefs)) {
            return Collections.emptyList();
        } else {
            LOG.debug("Read EM metadata for {} EM bodies", emBodyRefs.size());
            return HttpHelper.retrieveData(httpClient.target(dataServiceURL)
                            .path("/emdata/emBodies")
                            .queryParam("refs", emBodyRefs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)),
                    authorization,
                    new TypeReference<List<CDMIPBody>>() {},
                    Collections.emptyList());
        }
    }

    public Map<String, ColorDepthMIP> retrieveCDMIPs(Set<String> mipIds) {
        if (CollectionUtils.isEmpty(mipIds)) {
            return Collections.emptyMap();
        } else {
            List<ColorDepthMIP> colorDepthMIPS = httpRetrieveCDMIPs(HttpHelper.createClient(), mipIds);
            Set<String> lmSamplesToRetrieve = new HashSet<>();
            Set<String> emBodiesToRetrieve = new HashSet<>();
            colorDepthMIPS.forEach(cdmip -> {
                if (cdmip.needsEMBody()) {
                    emBodiesToRetrieve.add(cdmip.emBodyRef);
                } else if (cdmip.needsLMSample()) {
                    lmSamplesToRetrieve.add(cdmip.sampleRef);
                }
            });
            Client httpClient = HttpHelper.createClient();
            Map<String, CDMIPSample> lmSamples = CDMIPSample.indexByRef(
                    httpRetrieveLMSamplesByRefs(httpClient, lmSamplesToRetrieve));
            Map<String, CDMIPBody> emBodies = CDMIPBody.indexByRef(
                    httpRetrieveEMBodiesByRefs(httpClient, emBodiesToRetrieve));
            return colorDepthMIPS.stream()
                    .peek(cdmip -> {
                        if (cdmip.needsEMBody()) {
                            cdmip.emBody = emBodies.get(cdmip.emBodyRef);
                        } else if (cdmip.needsLMSample()) {
                            cdmip.sample = lmSamples.get(cdmip.sampleRef);
                        }
//!!!!                        update3DStack(httpClient, cdmip);
                    })
                    .collect(Collectors.toMap(n -> n.id, n -> n));
        }
    }

    private List<ColorDepthMIP> httpRetrieveCDMIPs(Client httpClient, Set<String> mipIds) {
        LOG.debug("Read {} MIPs", mipIds.size());
        return HttpHelper.retrieveData(httpClient.target(dataServiceURL)
                        .path("/data/colorDepthMIPsWithSamples")
                        .queryParam("id", mipIds.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(null)),
                authorization,
                new TypeReference<List<ColorDepthMIP>>() {},
                Collections.emptyList());
    }

    public Map<String, String> retrieveLibraryNameMapping() {
        Map<String, Object> configJSON = HttpHelper.retrieveData(HttpHelper.createClient().target(configURL)
                        .path("/cdm_library"),
                null, // this does not require any authorization
                new TypeReference<Map<String, Object>>() {},
                Collections.emptyMap());
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
