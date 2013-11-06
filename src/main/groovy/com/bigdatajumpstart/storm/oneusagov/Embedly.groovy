package com.bigdatajumpstart.storm.oneusagov

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper

@JsonIgnoreProperties(ignoreUnknown = true)
class Embedly implements Serializable {

    String url

    @JsonProperty("provider_url")
    String providerUrl

    String description
    String title

    @JsonProperty("author_name")
    String authorName

    int height

    @JsonProperty("thumbnail_width")
    int thumbnailWidth

    int width

    String html

    @JsonProperty("author_url")
    String authorUrl

    String version

    @JsonProperty("provider_name")
    String providerName

    @JsonProperty("thumbnail_url")
    String thumbnailUrl

    String type

    @JsonProperty("thumbnail_height")
    int thumbnailHeight

    @Override
    String toString() {
        ObjectMapper jsonObjectMapper = new ObjectMapper()
        return jsonObjectMapper.writeValueAsString(this)
    }
}
