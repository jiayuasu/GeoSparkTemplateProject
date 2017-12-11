import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}


object ScalaExample extends App{

	var sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
		config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
		master("local[*]").appName("GeoSparkSQL-demo").getOrCreate()
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)


	val resourceFolder = System.getProperty("user.dir")+"/src/test/resources/"

  val csvPolygonInputLocation = resourceFolder + "testenvelope.csv"
  val csvPointInputLocation = resourceFolder + "testpoint.csv"
  val shapefileInputLocation = resourceFolder + "shapefiles/dbf"

  testPredicatePushdownAndRangeJonQuery()
  testDistanceJoinQuery()
  testAggregateFunction()
  testShapefileConstructor()

  System.out.println("All GeoSparkSQL DEMOs passed!")

  def testPredicatePushdownAndRangeJonQuery():Unit =
  {
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    println(geosparkConf)

    var polygonCsvDf = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPolygonInputLocation)
    polygonCsvDf.createOrReplaceTempView("polygontable")
    polygonCsvDf.show()
    var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20)), \"mypolygonid\") as polygonshape from polygontable")
    polygonDf.createOrReplaceTempView("polygondf")
    polygonDf.show()

    var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    pointCsvDF.show()
    var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    pointDf.show()

    var rangeJoinDf = sparkSession.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
      "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

    rangeJoinDf.explain()
    rangeJoinDf.show(3)
    assert (rangeJoinDf.count()==500)
  }

  def testDistanceJoinQuery(): Unit =
  {
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    println(geosparkConf)

    var pointCsvDF1 = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF1.createOrReplaceTempView("pointtable")
    pointCsvDF1.show()
    var pointDf1 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape1 from pointtable")
    pointDf1.createOrReplaceTempView("pointdf1")
    pointDf1.show()

    var pointCsvDF2 = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF2.createOrReplaceTempView("pointtable")
    pointCsvDF2.show()
    var pointDf2 = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as pointshape2 from pointtable")
    pointDf2.createOrReplaceTempView("pointdf2")
    pointDf2.show()

    var distanceJoinDf = sparkSession.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")
    distanceJoinDf.explain()
    distanceJoinDf.show(10)
    assert (distanceJoinDf.count()==2998)
  }

  def testAggregateFunction(): Unit =
  {
    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    println(geosparkConf)

    var pointCsvDF = sparkSession.read.format("csv").option("delimiter",",").option("header","false").load(csvPointInputLocation)
    pointCsvDF.createOrReplaceTempView("pointtable")
    var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20)), \"myPointId\") as arealandmark from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    var boundary = sparkSession.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")
    val coordinates:Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(1.1,101.1)
    coordinates(1) = new Coordinate(1.1,1100.1)
    coordinates(2) = new Coordinate(1000.1,1100.1)
    coordinates(3) = new Coordinate(1000.1,101.1)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    geometryFactory.createPolygon(coordinates)
    assert(boundary.take(1)(0).get(0)==geometryFactory.createPolygon(coordinates))
  }

  def testShapefileConstructor(): Unit =
  {
    var spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
    var shapefileDf = Adapter.toDf(spatialRDD,sparkSession)
    shapefileDf.show()
  }
}