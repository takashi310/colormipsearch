package org.janelia.colormipsearch.cmd.jacsdata;

import java.io.File;
import java.io.Serializable;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.Gender;

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

    public String emBodyName() {
        return emBody != null ? emBody.name : null;
    }

    public String emGender() {
        return emBody != null && emBody.emDataSet != null ? emBody.emDataSet.gender : null;
    }

    public String emSWCFilename(String prefix) {
        if (StringUtils.isBlank(emSWCFile)) {
            return null;
        } else {
            String fname = emBodyId() + ".swc";
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

        if (sample == null || StringUtils.isBlank(sample.publishingName)) {
            lmNeuron.setUnpublished(true);
        }
    }

    public void updateEMNeuron(EMNeuronMetadata emNeuron) {
        emNeuron.setPublishedName(emBodyId());
        emNeuron.setAnatomicalArea(anatomicalArea);
        emNeuron.setNeuronInstance(neuronInstance);
        emNeuron.setNeuronType(neuronType);
        emNeuron.setGender(Gender.fromVal(emGender()));
        // set neuron files
        emNeuron.setNeuronFile(FileType.ColorDepthMip, publicImageUrl);
        emNeuron.setNeuronFile(FileType.ColorDepthMipThumbnail, publicThumbnailUrl);
        emNeuron.setNeuronFile(FileType.AlignedBodySWC, emSWCFilename(emNeuron.getLibraryName()));

        if (bodyId == null) {
            emNeuron.setUnpublished(true);
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
