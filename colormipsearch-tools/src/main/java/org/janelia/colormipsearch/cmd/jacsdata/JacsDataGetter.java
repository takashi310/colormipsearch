package org.janelia.colormipsearch.cmd.jacsdata;

import java.util.Collection;
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
import org.janelia.colormipsearch.dao.PublishedLMImageDao;
import org.janelia.colormipsearch.dao.PublishedURLsDao;
import org.janelia.colormipsearch.model.AbstractBaseEntity;
import org.janelia.colormipsearch.model.PublishedLMImage;
import org.janelia.colormipsearch.model.PublishedURLs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacsDataGetter {
    private static final Logger LOG = LoggerFactory.getLogger(JacsDataGetter.class);

    private final PublishedLMImageDao publishedLMImageDao;
    private final PublishedURLsDao publishedURLsDao;
    private final String dataServiceURL;
    private final String configURL;
    private final String authorization;
    private final int readBatchSize;
    private final Map<String, Set<String>> publishedAlignmentSpaceAliases;

    public JacsDataGetter(PublishedLMImageDao publishedLMImageDao,
                          PublishedURLsDao publishedURLsDao,
                          String dataServiceURL,
                          String configURL,
                          String authorization,
                          int readBatchSize,
                          Map<String, Set<String>> publishedAlignmentSpaceAliases) {
        this.publishedLMImageDao = publishedLMImageDao;
        this.publishedURLsDao = publishedURLsDao;
        this.dataServiceURL = dataServiceURL;
        this.configURL = configURL;
        this.authorization = authorization;
        this.readBatchSize = readBatchSize;
        this.publishedAlignmentSpaceAliases = publishedAlignmentSpaceAliases;
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
            return HttpHelper.retrieveDataStreamForFieldValues(() -> httpClient.target(dataServiceURL)
                                    .path("/data/samples"),
                            authorization,
                            readBatchSize,
                            "name",
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
        return HttpHelper.retrieveDataStreamForFieldValues(
                () -> httpClient.target(dataServiceURL).path("/data/samples"),
                authorization,
                readBatchSize,
                "refs",
                sampleRefs,
                new TypeReference<List<CDMIPSample>>() {})
                .collect(Collectors.toList());
    }

    private PublishedLMImage findPublishedImage(ColorDepthMIP colorDepthMIP, List<PublishedLMImage> publishedLMImages) {
        if (CollectionUtils.isEmpty(publishedLMImages)) {
            return new PublishedLMImage();
        } else {
            Set<String> aliasesForAlignmentSpace = publishedAlignmentSpaceAliases.getOrDefault(
                    colorDepthMIP.alignmentSpace,
                    Collections.emptySet());
            return publishedLMImages.stream()
                    .filter(pi -> pi.getAlignmentSpace().equals(colorDepthMIP.alignmentSpace) ||
                            (CollectionUtils.isNotEmpty(aliasesForAlignmentSpace) && aliasesForAlignmentSpace.contains(pi.getAlignmentSpace())))
                    .filter(pi -> pi.getObjective().equals(colorDepthMIP.objective))
                    .findFirst()
                    .orElse(new PublishedLMImage());
        }
    }

    private void update3DStack(ColorDepthMIP colorDepthMIP, PublishedLMImage publishedLMImage) {
        if (colorDepthMIP.sample != null) {
            colorDepthMIP.sample3DImageStack = publishedLMImage.getFile("VisuallyLosslessStack");
            colorDepthMIP.sampleGen1Gal4ExpressionImage = publishedLMImage.getGal4Expression4Image(colorDepthMIP.anatomicalArea);
        } else if (colorDepthMIP.emBody != null && colorDepthMIP.emBody.files != null) {
            colorDepthMIP.emSWCFile = colorDepthMIP.emBody.files.get("SkeletonSWC");
            colorDepthMIP.emOBJFile = colorDepthMIP.emBody.files.get("SkeletonOBJ");
        }
    }

    private List<CDMIPBody> httpRetrieveEMNeuronsByDatasetAndBodyIds(Client httpClient,
                                                                     String emDataset,
                                                                     String emDatasetVersion,
                                                                     Set<String> neuronBodyIds) {
        if (CollectionUtils.isEmpty(neuronBodyIds)) {
            return Collections.emptyList();
        } else {
            LOG.debug("Read EM metadata for {} neurons", neuronBodyIds.size());
            return HttpHelper.retrieveDataStreamForFieldValues(() -> httpClient.target(dataServiceURL)
                            .path("/emdata/dataset")
                            .path(emDataset)
                            .path(emDatasetVersion),
                    authorization,
                    readBatchSize,
                    "name",
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
            return HttpHelper.retrieveDataStreamForFieldValues(() -> httpClient.target(dataServiceURL)
                            .path("/emdata/emBodies"),
                    authorization,
                    readBatchSize,
                    "refs",
                    emBodyRefs,
                    new TypeReference<List<CDMIPBody>>() {
                    }).collect(Collectors.toList());
        }
    }

    public Map<String, ColorDepthMIP> retrieveCDMIPs(Set<String> mipIds) {
        if (CollectionUtils.isEmpty(mipIds)) {
            return Collections.emptyMap();
        } else {
            List<ColorDepthMIP> colorDepthMIPS = httpRetrieveCDMIPs(HttpHelper.createClient(), mipIds);
            Set<String> lmSamplesToRetrieve = new HashSet<>();
            Set<String> emBodiesToRetrieve = new HashSet<>();
            Map<String, List<ColorDepthMIP>> mipsGroupedByAlignmentSpace = colorDepthMIPS.stream()
                    .filter(cdmip -> StringUtils.isNotBlank(cdmip.sampleRef))
                    .collect(Collectors.groupingBy(
                            cdmip -> cdmip.alignmentSpace,
                            Collectors.toList())
                    );
            Map<String, List<PublishedLMImage>> publishedImagesBySampleRefsForAllAlignmentSpaces = mipsGroupedByAlignmentSpace.entrySet().stream()
                            .flatMap(e -> {
                                LOG.info("Retrieve {} published images for alignment space {}",
                                        mipIds.size(),
                                        e.getKey()); // alignment space
                                return publishedLMImageDao.getPublishedImagesWithGal4BySampleObjectives(
                                        null,
                                        e.getValue().stream().map(cdmip -> cdmip.sampleRef).collect(Collectors.toSet()),
                                        null
                                ).values().stream().flatMap(Collection::stream);
                            })
                    .collect(Collectors.groupingBy(PublishedLMImage::getSampleRef, Collectors.toList()));
            colorDepthMIPS.forEach(cdmip -> {
                if (cdmip.needsEMBody()) {
                    emBodiesToRetrieve.add(cdmip.emBodyRef);
                } else if (cdmip.needsLMSample()) {
                    lmSamplesToRetrieve.add(cdmip.sampleRef);
                }
            });
            Client httpClient = HttpHelper.createClient();
            LOG.info("Retrieve {} LM samples", lmSamplesToRetrieve.size());
            Map<String, CDMIPSample> lmSamples = CDMIPSample.indexByRef(
                    httpRetrieveLMSamplesByRefs(httpClient, lmSamplesToRetrieve));
            LOG.info("Retrieve {} EM bodies", emBodiesToRetrieve.size());
            Map<String, CDMIPBody> emBodies = CDMIPBody.indexByRef(
                    httpRetrieveEMBodiesByRefs(httpClient, emBodiesToRetrieve));
            LOG.info("Update 3D stack info for {} mips", colorDepthMIPS.size());
            return colorDepthMIPS.stream()
                    .peek(cdmip -> {
                        if (cdmip.needsEMBody()) {
                            cdmip.emBody = emBodies.get(cdmip.emBodyRef);
                        } else if (cdmip.needsLMSample()) {
                            cdmip.sample = lmSamples.get(cdmip.sampleRef);
                        }
                        update3DStack(cdmip, findPublishedImage(cdmip, publishedImagesBySampleRefsForAllAlignmentSpaces.get(cdmip.sampleRef)));
                    })
                    .collect(Collectors.toMap(n -> n.id, n -> n));
        }
    }

    public Map<String, List<PublishedLMImage>> retrievePublishedImages(String alignmentSpace, Set<String> sampleRefs) {
        return publishedLMImageDao.getPublishedImagesWithGal4BySampleObjectives(
                alignmentSpace,
                sampleRefs,
                null
        );
    }

    public Map<Number, PublishedURLs> retrievePublishedURLs(Set<Number> neuronIds) {
        return publishedURLsDao.findByEntityIds(neuronIds).stream()
                .collect(Collectors.toMap(AbstractBaseEntity::getEntityId, urls -> urls));
    }

    private List<ColorDepthMIP> httpRetrieveCDMIPs(Client httpClient, Set<String> mipIds) {
        LOG.debug("Read {} MIPs", mipIds.size());
        return HttpHelper.retrieveDataStreamForFieldValues(
                        () -> httpClient.target(dataServiceURL).path("/data/colorDepthMIPsWithSamples"),
                        authorization,
                        readBatchSize,
                        "id",
                        mipIds,
                        new TypeReference<List<ColorDepthMIP>>() {})
                .collect(Collectors.toList());
    }

    public Map<String, String> retrieveLibraryNameMapping() {
        Map<String, Object> configJSON = HttpHelper.retrieveData(
                HttpHelper.createClient().target(configURL).path("/cdm_library"),
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
