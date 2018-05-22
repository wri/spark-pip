package com.esri 
import com.vividsolutions.jts.geom._ 
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory 
import org.apache.spark.{SparkConf, SparkContext} 
import org.geotools.geometry.jts.WKTReader2 
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD


object MainApp extends App {
  val sparkConf = new SparkConf()
    .setAppName("Spark PiP")
    .registerKryoClasses(Array(
      classOf[Feature],
      classOf[FeaturePoint],
      classOf[FeaturePolygon],
      classOf[Point],
      classOf[Polygon],
      classOf[RowCol]
    ))

  
  def matchTest(x: String): Int = x.toInt match {
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


  def groupRDDAndSave(analysisType: String, with_poly: RDD[Array[String]], outputPath: String) = {

    // look for either gain or extent, we treat them the same
    val Pattern = "(gain|extent)".r

    val df = analysisType match {
      case Pattern(analysisType) => processExtent(with_poly)
      case "loss" => processLoss(with_poly)
      case "biomass" => processBiomass(with_poly)
      case "fire" => processFire(with_poly)
      case _ => throw new IllegalArgumentException
    }

    df.write.format("csv").save(outputPath)

  }
    
  def biomass_per_pixel(biomass: String)(area: String): Double = { 
    biomass.toDouble * area.toDouble / 10000.0
  }

  val propFileName = if (args.length == 0) "application.properties" else args(0)
  AppProperties.loadProperties(propFileName, sparkConf)
  val sc = new SparkContext(sparkConf)
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  // I'm sure there's a better way to do this
  // but all the examples I've found use case classes to go from RDD -> DataFrame
  case class ExtentRow( polyname: String, boundary1: String, boundary2: String, boundary3: String, boundary4: String, iso: String, id1: String, id2: String, thresh: Long, area: Double)
  case class LossRow(polyname: String, boundary1: String, boundary2: String, boundary3: String, boundary4: String, iso: String, id1: String, id2: String, year: String, area: Double, thresh: Long, biomass: Double)
  case class BiomassRow( polyname: String, boundary1: String, boundary2: String, boundary3: String, boundary4: String, iso: String, id1: String, id2: String, biomass: Double)
  case class FireRow( acq_date: String, fire_type: String, polyname: String, boundary1: String, boundary2: String, iso: String, id1: String, id2: String)

  def processExtent(inRDD: RDD[Array[String]]): DataFrame = {
      inRDD.map({case Array(thresh, area, polyname, boundary1, boundary2, boundary3, boundary4, iso, id1, id2) =>
                         (ExtentRow(polyname, boundary1, boundary2, boundary3, boundary4, iso, id1, id2, matchTest(thresh), area.toDouble)) })
                    .toDF()
                    .groupBy("polyname", "boundary1", "boundary2", "boundary3", "boundary4", "iso", "id1", "id2", "thresh").agg(sum("area"))
    }

  def processLoss(inRDD: RDD[Array[String]]): DataFrame = {
      inRDD.map({case Array(year, area, thresh, biomass, polyname, boundary1, boundary2, boundary3, boundary4, iso, id1, id2) =>
                         (LossRow(polyname, boundary1, boundary2, boundary3, boundary4, iso, id1, id2, year, area.toDouble, matchTest(thresh), biomass_per_pixel(biomass)(area))) })
                    .toDF()
                    .groupBy("polyname", "boundary1", "boundary2", "boundary3", "boundary4", "iso", "id1", "id2", "thresh", "year").agg(sum("area"), sum("biomass"))
    }

  def processBiomass(inRDD: RDD[Array[String]]): DataFrame = {
      inRDD.map({case Array(raw_biomass, area, polyname, boundary1, boundary2, boundary3, boundary4, iso, id1, id2) =>
                           (BiomassRow(polyname, boundary1, boundary2, boundary3, boundary4, iso, id1, id2, biomass_per_pixel(raw_biomass)(area))) })
                    .toDF()
                    .groupBy("polyname", "boundary1", "boundary2", "boundary3", "boundary4", "iso", "id1", "id2").agg(sum("biomass"))
    }

  def processFire(inRDD: RDD[Array[String]]): DataFrame = {
     inRDD.map({case Array(lat, lon, acq_date, fire_type, polyname, boundary1, boundary2, boundary3, boundary4, iso, id1, id2) =>
                          (FireRow(acq_date, fire_type, polyname, boundary1, boundary2, iso, id1, id2 )) })
                    .toDF()
                    .groupBy("acq_date", "fire_type", "polyname", "boundary1", "boundary2", "iso", "id1", "id2").count()
    }


  try {
    val conf = sc.getConf

    val geomFact = new GeometryFactory(new PrecisionModel(conf.getDouble("geometry.precision", 1000000.0)))
    val minLon = conf.getDouble("extent.xmin", -180.0)
    val maxLon = conf.getDouble("extent.xmax", 180.0)
    val minLat = conf.getDouble("extent.ymin", -90.0)
    val maxLat = conf.getDouble("extent.ymax", 90.0)
    val envp = new Envelope(minLon, maxLon, minLat, maxLat)
    val pointSep = conf.get("points.sep", "\t")
    val pointLon = conf.getInt("points.x", 0)
    val pointLat = conf.getInt("points.y", 1)
    val pointIdx = conf.get("points.fields", "").split(',').map(_.toInt)
    val reduceSize = conf.getDouble("reduce.size", 1.0)
    val analysisType = conf.get("analysis.type", "")
    val outputPath = conf.get("output.path")

    val pointRDD = sc
      .textFile(conf.get("points.path"))
      .flatMap(line => {
        try {
          val splits = line.split(pointSep, -1)
          val lon = splits(pointLon).toDouble
          val lat = splits(pointLat).toDouble
          val geom = geomFact.createPoint(new Coordinate(lon, lat))
          if (geom.getEnvelopeInternal.intersects(envp)) {
            Some(FeaturePoint(geom, pointIdx.map(splits(_))))
          }
          else
            None
        }
        catch {
          case t: Throwable => {
            None
          }
        }
      })
      .flatMap(_.toRowCols(reduceSize))
    val polygonSep = conf.get("polygons.sep", "\t")
    val polygonWKT = conf.getInt("polygons.wkt", 0)
    val polygonIdx = conf.get("polygons.fields", "").split(',').map(_.toInt)
    val polygonRDD = sc
      .textFile(conf.get("polygons.path"))
      .mapPartitions(iter => {
        val wktReader = new WKTReader2(geomFact)
        iter.flatMap(line => {
          try {
            val splits = line.split(polygonSep, -1)
            val geom = wktReader.read(splits(polygonWKT))
            if (geom.getEnvelopeInternal.intersects(envp))
              Some(FeaturePolygon(geom, polygonIdx.map(splits(_))))
            else
              None
          }
          catch {
            case t: Throwable => {
              None
            }
          }
        })
      })
      .flatMap(_.toRowCols(reduceSize))

    val with_poly = pointRDD
      .cogroup(polygonRDD)
      .mapPartitions(iter => {
        val preparedGeometryFactory = new PreparedGeometryFactory()
        iter.flatMap {
          case (_, (points, polygons)) => {
            // Prepare the polygon for fast PiP
            val polygonArr = polygons
              .map(_.prepare(preparedGeometryFactory))
              .toArray
            // Cartesian product between the points and polygons.
            // Finding the points in the explict polygon shape, and appending the attributes if truly inside.
            points
              .flatMap(point => {
                polygonArr
                  .filter(_.contains(point.geom))
                  .map(polygon => {
                    point.attr ++ polygon.attr
                  })
              })
          }
        }
      })

      // if it's a known analysis, we'll do some more processing here in scala
      val validAnalyses = Array("extent", "gain", "loss", "biomass", "fire")
      if (validAnalyses contains analysisType) {
        groupRDDAndSave(analysisType, with_poly, outputPath)

      // otherwise we'll write the entire point + polygon attributes table to hadoop
      } else {
        with_poly
          .map(_.mkString(","))
          .saveAsTextFile(outputPath)
      }

  } finally {
    sc.stop()
  }
}


