package com.bigdatajumpstart.storm.oneusagov

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Maps 1.usa.gov JSON fields to a Java object
 *             see: http://www.usa.gov/About/developer-resources/1usagov.shtml
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class OneUsaGovRequest implements Serializable {

    @JsonProperty("a")
    String userAgent;

    @JsonProperty("c")
    String countryCode;

    @JsonProperty("nk")
    int knownUser;

    @JsonProperty("g")
    String globalBitlyHash;

    @JsonProperty("h")
    String encodingUserBitlyHash;

    @JsonProperty("l")
    String encodingUserLogin;

    @JsonProperty("hh")
    String shortUrlCName;

    @JsonProperty("r")
    String referringUrl;

    @JsonProperty("u")
    String longUrl;

    @JsonProperty("t")
    long timestamp;

    @JsonProperty("hc")
    long hashCreatedOn;

    @JsonProperty("gr")
    String geoRegion;

    @JsonProperty("tz")
    String timezone;

    @JsonProperty("cy")
    String geoCityName;

    @JsonProperty("al")
    String acceptLanguage;

    @JsonProperty("ll")
    double[] latlon;

    @Override
    String toString() {
        ObjectMapper jsonObjectMapper = new ObjectMapper()
        return jsonObjectMapper.writeValueAsString(this)
    }
}
