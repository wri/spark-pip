package com.esri
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

  
object Summary {

  def groupRDDAndSave(analysisType: String, with_poly: RDD[Array[String]], outputPath: String)( implicit sqlContext: SQLContext ) = {

    // look for either gain or extent, we treat them the same
    val Pattern = "(gain|extent)".r

    val df = analysisType match {
      case Pattern(analysisType) => processExtent(with_poly)
      case "loss" => processLoss(with_poly)
      case "biomass" => processBiomass(with_poly)
      case "fire" => processFire(with_poly)
      case "glad" => processGLAD(with_poly)
      case _ => throw new IllegalArgumentException
    }

    df.write.format("csv").save(outputPath)

  }
    
  def processExtent(inRDD: RDD[Array[String]])(implicit sqlContext: SQLContext): DataFrame = {

    // unclear how best to avoid these multiple sqlContext.implicits_ imports
    // this could help: https://stackoverflow.com/questions/43215831/
    // but I'd need different functions to map RDD -> case classes, then bring back to convert to DF,
    // then go back to table-specific functions again.
    import sqlContext.implicits._
    
    inRDD.map({case Array(thresh, area, polyname, bound1, bound2, bound3, bound4, iso, id1, id2) =>
              (ExtentRow(polyname, bound1, bound2, bound3, bound4, iso, id1, id2, HansenUtils.matchTest(thresh), area.toDouble)) })
              .toDF()
              .groupBy("polyname", "bound1", "bound2", "bound3", "bound4", "iso", "id1", "id2", "thresh")
              .agg(sum("area"))
    }

  def processLoss(inRDD: RDD[Array[String]])(implicit sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    inRDD.map({case Array(year, area, thresh, biomass, polyname, bound1, bound2, bound3, bound4, iso, id1, id2) =>
              (LossRow(polyname, bound1, bound2, bound3, bound4, iso, id1, id2, 
                       year, area.toDouble, HansenUtils.matchTest(thresh), HansenUtils.biomass_per_pixel(biomass)(area))) })
              .toDF()
              .groupBy("polyname", "bound1", "bound2", "bound3", "bound4", "iso", "id1", "id2", "thresh", "year")
              .agg(sum("area"), sum("biomass"))
    }

  def processBiomass(inRDD: RDD[Array[String]])(implicit sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    inRDD.map({case Array(raw_biomass, area, polyname, bound1, bound2, bound3, bound4, iso, id1, id2) =>
              (BiomassRow(polyname, bound1, bound2, bound3, bound4, iso, id1, id2, 
                          HansenUtils.biomass_per_pixel(raw_biomass)(area))) })
              .toDF()
              .groupBy("polyname", "bound1", "bound2", "bound3", "bound4", "iso", "id1", "id2")
              .agg(sum("biomass"))
    }

  def processFire(inRDD: RDD[Array[String]])(implicit sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    inRDD.map({case Array(lat, lon, acq_date, fire_type, polyname, bound1, bound2, bound3, bound4, iso, id1, id2) =>
              (FireRow(acq_date, fire_type, polyname, bound1, bound2, iso, id1, id2 )) })
              .toDF()
              .groupBy("acq_date", "fire_type", "polyname", "bound1", "bound2", "iso", "id1", "id2")
              .count()
    }

  def processGLAD(inRDD: RDD[Array[String]])(implicit sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    inRDD.map({case Array(lat, lon, confidence, year, julian_day, area_m2, emissions, climate_mask, polyname, bound1, bound2, iso, id1, id2) =>
              (GLADRow(confidence, year, julian_day, climate_mask, polyname, bound1, bound2, iso, id1, id2, area_m2.toDouble, emissions.toDouble )) })
              .toDF()
              .groupBy("confidence", "year", "julian_day", "climate_mask", "polyname", "bound1", "bound2", "iso", "id1", "id2")
              .count()
              .agg(sum("area_m2"), sum("emissions"))
    }

  case class ExtentRow( polyname: String, bound1: String, bound2: String, bound3: String, bound4: String, 
                        iso: String, id1: String, id2: String, thresh: Long, area: Double )

  case class LossRow( polyname: String, bound1: String, bound2: String, bound3: String, bound4: String, 
                      iso: String, id1: String, id2: String, year: String, area: Double, thresh: Long, biomass: Double )

  case class BiomassRow( polyname: String, bound1: String, bound2: String, bound3: String, bound4: String, 
                         iso: String, id1: String, id2: String, biomass: Double )
  
  case class FireRow( acq_date: String, fire_type: String, polyname: String, bound1: String, bound2: String, 
                      iso: String, id1: String, id2: String )

  case class GLADRow( confidence: String, year: String, julian_day: String, climate_mask: String, polyname: String, 
                      bound1: String, bound2: String, iso: String, id1: String, id2: String, area_m2: Double, emissions: Double )


}
