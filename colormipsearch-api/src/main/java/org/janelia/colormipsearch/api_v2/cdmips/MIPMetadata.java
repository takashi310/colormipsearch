package org.janelia.colormipsearch.api_v2.cdmips;

public class MIPMetadata extends AbstractMetadata {

    public MIPMetadata variantAsMIP(String variant) {
        if (hasVariant(variant)) {
            MIPMetadata variantMIP = new MIPMetadata();
            copyTo(variantMIP);
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
