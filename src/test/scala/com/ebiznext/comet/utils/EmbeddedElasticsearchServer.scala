package com.ebiznext.comet.utils

import java.util.concurrent.TimeUnit

import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}

class EmbeddedElasticsearchServer(var storagePath: String) {
  val ELASTIC_VERSION = "6.4.2"
  val TRANSPORT_TCP_PORT = 9300

  val embeddedElastic = EmbeddedElastic
    .builder()
    .withElasticVersion(ELASTIC_VERSION)
    .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9300)
    .withSetting(PopularProperties.CLUSTER_NAME, "CometESCluster")
    .withStartTimeout(1, TimeUnit.MINUTES)
    .build()

  def start(): EmbeddedElastic = embeddedElastic.start()
  def stop(): Unit = embeddedElastic.stop()
}
