package org.janelia.colormipsearch.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used only for the schema generation to mark required fields.
 * This was needed because the intermediate results have some fields in the JSON serialization
 * that are needed during the computation process but in the final results uploaded to AWS
 * we only want to keep fields used by the NeuronBridge application and the JSON schema only
 * defines the attributes uploaded to AWS and used by NeuronBridge.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonRequired {
}
