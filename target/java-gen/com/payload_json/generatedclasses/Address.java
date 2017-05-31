
package com.payload_json.generatedclasses;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;


/**
 * An Address following the convention of http://microformats.org/wiki/hcard
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "post-office-box",
    "extended-address",
    "street-address",
    "locality",
    "region",
    "postal-code",
    "country-name"
})
public class Address {

    @JsonProperty("post-office-box")
    private String postOfficeBox;
    @JsonProperty("extended-address")
    private String extendedAddress;
    @JsonProperty("street-address")
    private String streetAddress;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("locality")
    private String locality;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("region")
    private String region;
    @JsonProperty("postal-code")
    private String postalCode;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("country-name")
    private String countryName;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("post-office-box")
    public String getPostOfficeBox() {
        return postOfficeBox;
    }

    @JsonProperty("post-office-box")
    public void setPostOfficeBox(String postOfficeBox) {
        this.postOfficeBox = postOfficeBox;
    }

    @JsonProperty("extended-address")
    public String getExtendedAddress() {
        return extendedAddress;
    }

    @JsonProperty("extended-address")
    public void setExtendedAddress(String extendedAddress) {
        this.extendedAddress = extendedAddress;
    }

    @JsonProperty("street-address")
    public String getStreetAddress() {
        return streetAddress;
    }

    @JsonProperty("street-address")
    public void setStreetAddress(String streetAddress) {
        this.streetAddress = streetAddress;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("locality")
    public String getLocality() {
        return locality;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("locality")
    public void setLocality(String locality) {
        this.locality = locality;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("region")
    public String getRegion() {
        return region;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("region")
    public void setRegion(String region) {
        this.region = region;
    }

    @JsonProperty("postal-code")
    public String getPostalCode() {
        return postalCode;
    }

    @JsonProperty("postal-code")
    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("country-name")
    public String getCountryName() {
        return countryName;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("country-name")
    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(postOfficeBox).append(extendedAddress).append(streetAddress).append(locality).append(region).append(postalCode).append(countryName).append(additionalProperties).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Address) == false) {
            return false;
        }
        Address rhs = ((Address) other);
        return new EqualsBuilder().append(postOfficeBox, rhs.postOfficeBox).append(extendedAddress, rhs.extendedAddress).append(streetAddress, rhs.streetAddress).append(locality, rhs.locality).append(region, rhs.region).append(postalCode, rhs.postalCode).append(countryName, rhs.countryName).append(additionalProperties, rhs.additionalProperties).isEquals();
    }

}
