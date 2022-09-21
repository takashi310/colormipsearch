package org.janelia.colormipsearch.cmd.jacsdata;

import java.io.Serializable;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.ProcessingType;

/**
 * This is the representation of a JACS ColorDepthMIP image.
 */
public class ColorDepthMIP implements Serializable {
    @JsonProperty("_id")
    public String id;
    @JsonProperty
    public String name;
    @JsonProperty
    public String filepath;
    @JsonProperty
    public String objective;
    @JsonProperty
    public String alignmentSpace;
    @JsonProperty
    public String anatomicalArea;
    @JsonProperty
    public String channelNumber;
    @JsonProperty
    public Long bodyId;
    @JsonProperty
    public String neuronType;
    @JsonProperty
    public String neuronInstance;
    @JsonProperty
    public String publicImageUrl;
    @JsonProperty
    public String publicThumbnailUrl;
    @JsonProperty
    public Set<String> libraries;
    @JsonProperty
    public String sampleRef;
    @JsonProperty
    public CDMIPSample sample;
    @JsonProperty
    public String emBodyRef;
    @JsonProperty
    public CDMIPBody emBody;
    public String sample3DImageStack;
    public String sampleGen1Gal4ExpressionImage;
    public String emSWCFile;
    public String emOBJFile;

    public String findLibrary(String libraryName) {
        if (CollectionUtils.isEmpty(libraries)) {
            return null;
        } else {
            // since a MIP may be in multiple libraries we want to make sure we have the one that we requested the mip for
            return libraries.stream()
                    .filter(StringUtils::isNotBlank)
                    .filter(l -> l.equals(libraryName))
                    .findFirst().orElse(null);
        }
    }

    public int channelNumber() {
        return StringUtils.isNotBlank(channelNumber) ? Integer.parseInt(channelNumber) : -1;
    }

    public String lmLineName() {
        return sample != null ? sample.publishingName : null;
    }

    public String lmSlideCode() {
        return sample != null ? sample.slideCode : null;
    }

    public String lmMountingProtocol() {
        return sample != null ? sample.mountingProtocol : null;
    }

    public String lmDriver() {
        return sample != null ? sample.driver : null;
    }

    public String lmObjective() {
        return objective;
    }

    public String emBodyId() {
        return bodyId != null ? bodyId.toString() : null;
    }

    public String emPublishedName() {
        if (emBody == null) {
            return emBodyId();
        } else {
            if (StringUtils.isNotBlank(emBody.datasetIdentifier)) {
                return emBody.datasetIdentifier + ":" + emBodyId();
            } else {
                return emBodyId();
            }
        }
    }

    public String emGender() {
        return emBody != null && emBody.emDataSet != null ? emBody.emDataSet.gender : null;
    }

    public String emAnatomicalArea() {
        return emBody != null && emBody.emDataSet != null
                ? emBody.emDataSet.anatomicalArea
                : anatomicalArea;
    }
    public String em3DFilename(String prefix, String em3DFile, String ext) {
        if (StringUtils.isBlank(em3DFile)) {
            return null;
        } else {
            String fname = emBodyId() + ext;
            if (StringUtils.isBlank(prefix)) {
                return fname;
            } else {
                return prefix + "/" + fname;
            }
        }
    }

    public String gender() {
        if (sample != null) {
            return sample.gender;
        } else {
            return null;
        }
    }

    public boolean needsLMSample() {
        return StringUtils.isNotBlank(sampleRef) && sample == null;
    }

    public boolean needsEMBody() {
        return StringUtils.isNotBlank(emBodyRef) && emBody == null;
    }

    public void updateLMNeuron(LMNeuronMetadata lmNeuron) {
        lmNeuron.setPublishedName(lmLineName());
        lmNeuron.setGender(Gender.fromVal(gender()));
        lmNeuron.setSlideCode(lmSlideCode());
        lmNeuron.setAnatomicalArea(anatomicalArea);
        lmNeuron.setMountingProtocol(lmMountingProtocol());
        lmNeuron.setObjective(lmObjective());
        lmNeuron.setChannel(channelNumber());
        // set neuron files
        lmNeuron.setNeuronFile(FileType.ColorDepthMip, publicImageUrl);
        lmNeuron.setNeuronFile(FileType.ColorDepthMipThumbnail, publicThumbnailUrl);
        lmNeuron.setNeuronFile(FileType.VisuallyLosslessStack, sample3DImageStack);
        lmNeuron.setNeuronFile(FileType.Gal4Expression, sampleGen1Gal4ExpressionImage);
        updateResultsFiles(lmNeuron);

        if (sample == null || StringUtils.isBlank(sample.publishingName)) {
            lmNeuron.setUnpublished(true);
        }
    }

    public void updateEMNeuron(EMNeuronMetadata emNeuron) {
        emNeuron.setPublishedName(emBodyId());
        emNeuron.setFullPublishedName(emPublishedName());
        emNeuron.setAnatomicalArea(emAnatomicalArea());
        emNeuron.setNeuronInstance(neuronInstance);
        emNeuron.setNeuronType(neuronType);
        emNeuron.setGender(Gender.fromVal(emGender()));
        // set neuron files
        emNeuron.setNeuronFile(FileType.ColorDepthMip, publicImageUrl);
        emNeuron.setNeuronFile(FileType.ColorDepthMipThumbnail, publicThumbnailUrl);
        emNeuron.setNeuronFile(FileType.AlignedBodySWC, em3DFilename(emNeuron.getLibraryName(), emSWCFile, ".swc"));
        emNeuron.setNeuronFile(FileType.AlignedBodyOBJ, em3DFilename(emNeuron.getLibraryName(), emOBJFile, ".obj"));
        updateResultsFiles(emNeuron);

        if (bodyId == null) {
            emNeuron.setUnpublished(true);
        }
    }

    private void updateResultsFiles(AbstractNeuronMetadata n) {
        if (n.hasAnyProcessedTag(ProcessingType.ColorDepthSearch)) {
            n.setNeuronFile(FileType.CDSResults, n.getMipId() + ".json");
        }
        if (n.hasAnyProcessedTag(ProcessingType.PPPMatch)) {
            n.setNeuronFile(FileType.PPPMResults, n.getPublishedName() + ".json");
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .toString();
    }
}
