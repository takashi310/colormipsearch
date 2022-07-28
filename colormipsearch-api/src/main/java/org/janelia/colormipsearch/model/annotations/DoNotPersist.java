package org.janelia.colormipsearch.model.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is used to specify that an entity attribute field will not be persisted in the database.
 * As an example for the mask the target image entities, we will persist only the internal entity IDs
 * and not the entire subdocument.
 */
@Retention(RUNTIME)
@Target({
        ElementType.FIELD,
        ElementType.METHOD
})
public @interface DoNotPersist {
}
