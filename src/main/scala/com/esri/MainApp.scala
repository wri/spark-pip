package com.esri 
import com.vividsolutions.jts.geom._ 
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory 
import org.apache.spark.{SparkConf, SparkContext} 
import org.geotools.geometry.jts.WKTReader2 


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


  val propFileName = if (args.length == 0) "application.properties" else args(0)
  AppProperties.loadProperties(propFileName, sparkConf)
  val sc = new SparkContext(sparkConf)

  // initialize sqlContext - will be used if analysis.type set in application.properties
  implicit val sqlContext= new org.apache.spark.sql.SQLContext(sc)

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
      val validAnalyses = Array("extent", "gain", "loss", "biomass", "fire", "grossEmis", "netEmis")
      if (validAnalyses contains analysisType) {
        Summary.groupRDDAndSave(analysisType, with_poly, outputPath)

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

