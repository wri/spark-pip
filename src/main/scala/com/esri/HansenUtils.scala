package com.esri

object HansenUtils {
  
  def matchTest(x: String): Int = x.toInt match {
    case x if x <= 0 => -1
    case x if x <= 10 => 0
    case x if x <= 15 => 10
    case x if x <= 20 => 15
    case x if x <= 25 => 20
    case x if x <= 30 => 25
    case x if x <= 50 => 30
    case x if x <= 75 => 50
    case x if x <= 100 => 75
    case _ => -9999
  }

  // Woods Hole biomass 2000 v4 map biomass units: megagrams biomass/hectare
  // Units for biomass tiles that go into tsv creation: megagrams biomass/hectare
  // Units for biomass in tsvs that go into Hadoop: megagrams biomass/hectare
  // Units for biomass that come out of Hadoop: megagrams biomass/pixel
  // Thus, the below function converts megagrams biomass/hectare to megagrams biomass/pixel.
  def biomass_per_pixel(biomass: String)(area: String): Double = { 
    biomass.toDouble * area.toDouble / 10000.0
  }

}
