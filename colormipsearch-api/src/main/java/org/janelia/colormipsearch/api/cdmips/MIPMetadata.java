package org.janelia.colormipsearch.api.cdmips;

public class MIPMetadata extends AbstractMetadata {

    public MIPMetadata variantAsMIP(String variant) {
        if (hasVariant(variant)) {
            MIPMetadata variantMIP = new MIPMetadata();
            variantMIP.setId(getId());
            variantMIP.setCdmPath(getCdmPath());
            variantMIP.setImageName(getVariant(variant));
            variantMIP.setImageArchivePath(getVariant(variant + "ArchivePath"));
            variantMIP.setImageType(getVariant(variant + "EntryType"));
            return variantMIP;
        } else {
            return null;
        }
    }

}
