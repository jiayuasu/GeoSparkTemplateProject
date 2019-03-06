import java.awt.Color
import java.io.FileInputStream
import java.util.Properties

import com.vividsolutions.jts.geom.Envelope
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD, RectangleRDD}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparkviz.core.{ImageGenerator, ImageSerializableWrapper, RasterOverlayOperator}
import org.datasyslab.geosparkviz.extension.visualizationEffect.{ChoroplethMap, HeatMap, ScatterPlot}
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import org.datasyslab.geosparkviz.utils.{ColorizeOption, ImageType}


object ScalaExample extends App{
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	val sparkConf = new SparkConf().setAppName("GeoSparkVizDemo").set("spark.serializer", classOf[KryoSerializer].getName)
		.set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
		.setMaster("local[*]")
	val sparkContext = new SparkContext(sparkConf)

	val prop = new Properties()
	val resourcePath = "src/test/resources/"
	val demoOutputPath = "target/demo"
	var ConfFile = new FileInputStream(resourcePath + "babylon.point.properties")
	prop.load(ConfFile)
	val scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot"
	val heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap"
	val choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap"
	val parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrenderstitchheatmap"
	val earthdataScatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/earthdatascatterplot"
	val sqlApiOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/sql-heatmap"

	val PointInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
	val PointOffset = prop.getProperty("offset").toInt
	val PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
	val PointNumPartitions = prop.getProperty("numPartitions").toInt
	ConfFile = new FileInputStream(resourcePath + "babylon.rectangle.properties")
	prop.load(ConfFile)
	val RectangleInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
	val RectangleOffset = prop.getProperty("offset").toInt
	val RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
	val RectangleNumPartitions = prop.getProperty("numPartitions").toInt
	ConfFile = new FileInputStream(resourcePath + "babylon.polygon.properties")
	prop.load(ConfFile)
	val PolygonInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
	val PolygonOffset = prop.getProperty("offset").toInt
	val PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
	val PolygonNumPartitions = prop.getProperty("numPartitions").toInt
	ConfFile = new FileInputStream(resourcePath + "babylon.linestring.properties")
	prop.load(ConfFile)
	val LineStringInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
	val LineStringOffset = prop.getProperty("offset").toInt
	val LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
	val LineStringNumPartitions = prop.getProperty("numPartitions").toInt
	val USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000)
	val earthdataInputLocation = System.getProperty("user.dir") + "/src/test/resources/modis/modis.csv"
	val earthdataNumPartitions = 5
	val HDFIncrement = 5
	val HDFOffset = 2
	val HDFRootGroupName = "MOD_Swath_LST"
	val HDFDataVariableName = "LST"
	val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")
	val HDFswitchXY = true
	val urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/"

	if (buildScatterPlot(scatterPlotOutputPath) && buildHeatMap(heatMapOutputPath)
		&& buildChoroplethMap(choroplethMapOutputPath) && parallelFilterRenderStitch(parallelFilterRenderStitchOutputPath + "-stitched")
		&& parallelFilterRenderNoStitch(parallelFilterRenderStitchOutputPath) && earthdataVisualization(earthdataScatterPlotOutputPath)
		&& sqlApiVisualization(sqlApiOutputPath))
		System.out.println("All GeoSparkViz Demos have passed.")
	else System.out.println("GeoSparkViz Demos failed.")

	/**
		* Builds the scatter plot.
		*
		* @param outputPath the output path
		* @return true, if successful
		*/
	def buildScatterPlot(outputPath: String): Boolean = {
		val spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
		var visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false)
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
		visualizationOperator.Visualize(sparkContext, spatialRDD)
		var imageGenerator = new ImageGenerator
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
		visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, false, true)
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
		visualizationOperator.Visualize(sparkContext, spatialRDD)
		imageGenerator = new ImageGenerator
		imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, outputPath, ImageType.SVG)
		visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, true, true)
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
		visualizationOperator.Visualize(sparkContext, spatialRDD)
		imageGenerator = new ImageGenerator
		imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.distributedVectorImage, outputPath + "-distributed", ImageType.SVG)
		true
	}

	/**
		* Builds the heat map.
		*
		* @param outputPath the output path
		* @return true, if successful
		*/
	def buildHeatMap(outputPath: String): Boolean = {
		val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
		val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2)
		visualizationOperator.Visualize(sparkContext, spatialRDD)
		val imageGenerator = new ImageGenerator
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
		true
	}

	/**
		* Builds the choropleth map.
		*
		* @param outputPath the output path
		* @return true, if successful
		*/
	def buildChoroplethMap(outputPath: String): Boolean = {
		val spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions, StorageLevel.MEMORY_ONLY)
		val queryRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY)
		spatialRDD.spatialPartitioning(GridType.RTREE)
		queryRDD.spatialPartitioning(spatialRDD.grids)
		spatialRDD.buildIndex(IndexType.RTREE, true)
		val joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, true, false)
		val visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false)
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
		visualizationOperator.Visualize(sparkContext, joinResult)
		val frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false)
		frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true)
		frontImage.Visualize(sparkContext, queryRDD)
		val overlayOperator = new RasterOverlayOperator(visualizationOperator.rasterImage)
		overlayOperator.JoinImage(frontImage.rasterImage)
		val imageGenerator = new ImageGenerator
		imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, outputPath, ImageType.PNG)
		true
	}

	/**
		* Parallel filter render stitch.
		*
		* @param outputPath the output path
		* @return true, if successful
		*/
	def parallelFilterRenderStitch(outputPath: String): Boolean = {
		val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
		val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
		visualizationOperator.Visualize(sparkContext, spatialRDD)
		val imageGenerator = new ImageGenerator
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, outputPath, ImageType.PNG)
		true
	}

	/**
		* Parallel filter render no stitch.
		*
		* @param outputPath the output path
		* @return true, if successful
		*/
	def parallelFilterRenderNoStitch(outputPath: String): Boolean = {
		val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
		val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
		visualizationOperator.Visualize(sparkContext, spatialRDD)
		val imageGenerator = new ImageGenerator
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, outputPath, ImageType.PNG)
		true
	}

	def earthdataVisualization(outputPath: String): Boolean = {
		val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
			HDFDataVariableList, HDFDataVariableName, HDFswitchXY, urlPrefix)
		val spatialRDD = new PointRDD(sparkContext, earthdataInputLocation, earthdataNumPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
		val visualizationOperator = new ScatterPlot(1000, 600, spatialRDD.boundaryEnvelope, ColorizeOption.EARTHOBSERVATION, false, false)
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.BLUE, true)
		visualizationOperator.Visualize(sparkContext, spatialRDD)
		val imageGenerator = new ImageGenerator
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
		true
	}

	def sqlApiVisualization(outputPath: String): Boolean = {
		val sqlContext = new SQLContext(sparkContext)
		val spark = sqlContext.sparkSession
		GeoSparkSQLRegistrator.registerAll(spark)
		GeoSparkVizRegistrator.registerAll(spark)
		var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(PointInputLocation)
		pointDf.createOrReplaceTempView("pointtable")
		spark.sql(
			"""
				|CREATE OR REPLACE TEMP VIEW pointtable AS
				|SELECT ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as shape
				|FROM pointtable
			""".stripMargin)
		spark.sql(
			"""
				|CREATE OR REPLACE TEMP VIEW pointtable AS
				|SELECT *
				|FROM pointtable
				|WHERE ST_Contains(ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000),shape)
			""".stripMargin)
		spark.sql(
			"""
				|CREATE OR REPLACE TEMP VIEW pixels AS
				|SELECT pixel, shape FROM pointtable
				|LATERAL VIEW ST_Pixelize(shape, 256, 256, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) AS pixel
			""".stripMargin)
		spark.sql(
			"""
				|CREATE OR REPLACE TEMP VIEW pixelaggregates AS
				|SELECT pixel, count(*) as weight
				|FROM pixels
				|GROUP BY pixel
			""".stripMargin)
		spark.sql(
			"""
				|CREATE OR REPLACE TEMP VIEW images AS
				|SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates), 'red')) AS image
				|FROM pixelaggregates
			""".stripMargin)
		var image = spark.table("images").take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
		var imageGenerator = new ImageGenerator
		imageGenerator.SaveRasterImageAsLocalFile(image, outputPath, ImageType.PNG)
		spark.sql(
			"""
				|CREATE OR REPLACE TEMP VIEW imagestring AS
				|SELECT ST_EncodeImage(image)
				|FROM images
			""".stripMargin)
		spark.table("imagestring").show()
		true
	}


}