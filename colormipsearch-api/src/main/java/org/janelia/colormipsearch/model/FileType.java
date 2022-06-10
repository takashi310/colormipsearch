package org.janelia.colormipsearch.model;

public enum FileType {
    ColorDepthMip, // The CDM of the image. For PPPM, this is the best matching channel of the matching LM stack and called 'Best Channel CDM' in the NeuronBridge GUI.
    ColorDepthMipThumbnail, // The thumbnail sized version of the ColorDepthMip, if available.
    ColorDepthMipInput, // CDM-only. The actual color depth image that was input. 'Matched CDM' in the NeuronBridge GUI.
    ColorDepthMipMatch, // CDM-only. The actual color depth image that was matched. 'Matched CDM' in the NeuronBridge GUI.
    ColorDepthMipSkel, // PPPM-only. The CDM of the best matching channel with the matching LM segmentation fragments overlaid. 'LM - Best Channel CDM with EM overlay' in the NeuronBridge GUI.
    SignalMip, // PPPM-only. The full MIP of all channels of the matching sample. 'LM - Sample All-Channel MIP' in the NeuronBridge GUI.
    SignalMipMasked, // PPPM-only. LM signal content masked with the matching LM segmentation fragments. 'PPPM Mask' in the NeuronBridge GUI.
    SignalMipMaskedSkel, // PPPM-only. LM signal content masked with the matching LM segmentation fragments, overlaid with the EM skeleton. 'PPPM Mask with EM Overlay' in the NeuronBridge GUI.
    SignalMipExpression, // MCFO-only. A representative CDM image of the full expression of the line.
    VisuallyLosslessStack, // LMImage-only. An H5J 3D image stack of all channels of the LM image.
    AlignedBodySWC, // EMImage-only, A 3D SWC skeleton of the EM body in the alignment space.
    AlignedBodyOBJ // EMImage-only. A 3D OBJ representation of the EM body in the alignment space.
}
