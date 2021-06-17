package org.janelia.colormipsearch.api.pppsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.RegExUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawPPPMatchesReader {
    private static final Logger LOG = LoggerFactory.getLogger(RawPPPMatchesReader.class);

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public List<PPPMatch> readPPPMatches(String fn) {
        return readPPPMatches(new File(fn));
    }

    public List<PPPMatch> readPPPMatches(File f) {
        JsonNode jsonContent = readJSONFile(f);
        return StreamSupport.stream(Spliterators.spliterator(jsonContent.fields(), Long.MAX_VALUE, 0), false)
                .flatMap(emMatchEntry -> getLMMatches(emMatchEntry.getKey(),emMatchEntry.getValue()))
                .collect(Collectors.toList())
                ;
    }

    private Stream<PPPMatch> getLMMatches(String emFullName, JsonNode lmMatchesNode) {
        return StreamSupport.stream(Spliterators.spliterator(lmMatchesNode.fields(), Long.MAX_VALUE, 0), false)
                .map(lmMatchEntry -> {
                    PPPMatch pppMatch = new PPPMatch();
                    pppMatch.setFullEmName(emFullName);
                    pppMatch.setFullLmName(lmMatchEntry.getKey());
                    try {
                        JsonNode lmMatchContent = lmMatchEntry.getValue();
                        RawSkeletonMatches skeletonMatch = objectMapper.readValue(lmMatchContent.toString(), RawSkeletonMatches.class);
                        pppMatch.setAggregateCoverage(skeletonMatch.getAggregateCoverage());
                        pppMatch.setCoverageScore(skeletonMatch.getCoverageScore());
                        pppMatch.setMirrored(skeletonMatch.getMirrored());
                        pppMatch.setEmPPPRank(skeletonMatch.getRank());
                        pppMatch.setSkeletonMatches(getAllSkeletonMatches(skeletonMatch));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return pppMatch;
                })
                ;
    }

    private List<SkeletonMatch> getAllSkeletonMatches(RawSkeletonMatches rawSkeletonMatches) throws IOException {
        List<SkeletonMatch> skeletonMatches = new ArrayList<>();
        List<String> allSkeletonIds = objectMapper.readValue(normalizeArrayString(rawSkeletonMatches.getAllSkeletonIds()), new TypeReference<List<String>>() {
        });
        List<Double> allNBlastScores = objectMapper.readValue(normalizeArrayString(rawSkeletonMatches.getAllNBlastScores()), new TypeReference<List<Double>>() {
        });
        List<Double> allCoverageScores = objectMapper.readValue(normalizeArrayString(rawSkeletonMatches.getAllCoveragesScores()), new TypeReference<List<Double>>() {
        });
        List<short[]> allColors = objectMapper.readValue(rawSkeletonMatches.getAllColors(), new TypeReference<List<short[]>>() {
        });
        if (allSkeletonIds.size() != allNBlastScores.size()) {
            throw new IllegalArgumentException("The size of skeleton ids must match the size of nblast scores in: " + rawSkeletonMatches);
        }
        if (allSkeletonIds.size() != allCoverageScores.size()) {
            throw new IllegalArgumentException("The size of skeleton ids must match the size of coverage scores in: " + rawSkeletonMatches);
        }
        for (int i = 0; i < allSkeletonIds.size(); i++) {
            SkeletonMatch skeletonMatch = new SkeletonMatch();
            skeletonMatch.setId(allSkeletonIds.get(i));
            skeletonMatch.setNblastScore(allNBlastScores.get(i));
            skeletonMatch.setCoverage(allCoverageScores.get(i));
            if (i < allColors.size()) skeletonMatch.setColors(allColors.get(i));
            skeletonMatches.add(skeletonMatch);
        }
        return skeletonMatches;
    }

    private String normalizeArrayString(String s) {
        return s.replaceAll("\\[\\s+", "[")
                .replaceAll("\\s+]", "]")
                .replaceAll("\\.]", "]")
                .replaceAll("\\s+", ", ")
                .replaceAll("\\.,", ",")
                ;
    }

    private JsonNode readJSONFile(File f) {
        try {
            LOG.info("Reading {}", f);
            return objectMapper.readTree(f);
        } catch (IOException e) {
            LOG.error("Error reading json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }
}
