package org.janelia.colormipsearch.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public interface BaseEntity {
    Number getEntityId();

    void setEntityId(Number id);

    boolean hasEntityId();

    @JsonIgnore
    default String getEntityName() {
        return getClass().getSimpleName();
    }

    Date getCreatedDate();

    void setCreatedDate(Date createdDate);

    /**
     * This is in order to serialize the JSON type property. When entities have generics the class property is not serialized
     * Jackson's explanation is because type information is lost at runtime due to reification even though the class should be
     * the class of the object itself not of the reified types used as generics.
     * @return
     */
    @JsonProperty("class")
    default String getEntityClass() {
        return getClass().getName();
    }

    /**
     * Dummy setter just so a default mapper would be able to read this even if
     * DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES is not set.
     */
    default void setEntityClass(String entityClass) {
        // nothing to do here
    }

}
