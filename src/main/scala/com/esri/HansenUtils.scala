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

  def biomass_per_pixel(biomass: String)(area: String): Double = { 
    biomass.toDouble * area.toDouble / 10000.0
  }

}
