package com.bigdatajumpstart.storm.oneusagov

import org.springframework.web.client.RestTemplate
import org.springframework.beans.factory.annotation.Autowired

import com.codahale.metrics.Timer

/**
 * Call out to the embed.ly embed endpoint to get metadata for a link.
 */
class EmbedlyService {

    @Autowired
    RestTemplate restTemplate

    String apiKey

    Timer apiTimer = MetricsSupport.timer(EmbedlyService, "apiCall")

    Embedly getLinkMetadata(String link) {
        String urlEncoded = URLEncoder.encode(link,"UTF-8")
        URI uri = new URI("https://api.embed.ly/1/oembed?key=${apiKey}&url=${urlEncoded}")

        Embedly embedly = null
        MetricsSupport.withTimer(apiTimer, {
            embedly = restTemplate.getForObject(uri, Embedly);
        })
        return embedly
    }
}
