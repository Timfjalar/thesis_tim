import com.tim.thesisfeatureIdParser.FeatureIdParserConfig
import com.tim.thesisfeatureIdParser.shared.LazyLogger
import com.tim.thesisfeatureIdParser.utils.HashingUtils._
import com.tim.thesisfeatureIdParser.utils.CoordinateUtils._
import com.google.inject.{ImplementedBy, Singleton}
import com.acervera.osm4scala.spark.OsmSqlEntity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.{Row, SparkSession}
import play.api.libs.json.{JsArray, JsNumber, Json}

import scala.collection.{Seq, mutable}

@ImplementedBy(classOf[IdParserPipelineImpl])
trait IdParserPipeline extends Serializable {
  def run(ss: SparkSession, config: FeatureIdParserConfig): Unit
}

@Singleton
class IdParserPipelineImpl extends IdParserPipeline with LazyLogger {
  // Ignore list for data deemed to be incorrectly edited
  // or part of a duplicate set of features
  private val IGNORE_LIST: Set[String] = Set("1179806612", "339526239", "227809502", "748928443", "779557872", "1199450793", "801923621", "1206022973", "1193167653", "1152474407", "1193797027", "995982626", "1208492314", "995493213", "1212401525", "1016029391", "1214768970", "803554948", "399216372", "1155577534", "882851168", "705790083", "549418026", "998382269", "526728697", "282304481", "744452784", "748873679", "596271526", "1210115680", "428963119", "944049745", "337674921", "570679479", "839035960", "236108581", "143563389", "1179690040", "1196883838", "759078917", "915157849", "795267036", "692259231", "1171980186", "1218946132", "1079845348", "690838336", "499750330", "354535333", "665469121", "403342299", "1089873194", "332273991", "915157849", "795267022", "254322028", "1076981531", "4213557", "407739881", "1096476435", "1197103914", "557824582", "878569450", "999518643", "311138308", "1055375138", "406768534", "337674945", "1179806612", "1217087874", "784081581", "1216278538", "1180677353", "1218238128", "253959921", "441424101", "1215984484", "998382268", "747513409", "549656261", "628844721", "254322041", "787149539", "162580170", "1094456887", "777662597", "107673788", "179411207", "1057362146", "684508223", "1218238142", "1178513499", "1187612684", "1217087866", "979946631", "1217088639", "1216136962", "1218238121", "1215984483", "1214633966", "88462839", "1217087855", "1180677337", "972294599", "704247266", "1216278578", "559784451", "1216491429", "1217087857", "1216204640", "1214983336", "337674934", "890775231", "1206932964", "692599943", "1073565023", "1216472344", "337674924", "1215493666", "98497686", "337674922", "1033187975", "777662593", "253959894", "1217088656", "906698794", "1217125723", "1141049910", "773054990", "1217397812", "1033187977", "1216247248", "939118750", "1216204649", "972138772", "1218238150", "1116240992", "1217088663", "1216136974", "1208661147", "1088140552", "1216136965", "1206932973", "1218238127", "1066631808", "1218238130", "787149548", "254322090", "963606521", "1210058939", "1202214973", "526728701", "1216136978", "1008275973", "253959880", "360396478", "693439812", "1216228938", "1199677246", "956200543", "1056410441", "1188584182", "1202028440", "1015910857", "253959722", "1214974160", "317578519", "1218229860", "1081962571", "1217205659", "1215984468", "1180677349", "1161737957", "1209937491", "1047737080", "1214983349", "1167122270", "1027089598", "1214983350", "253959884", "337674942", "1029704673", "253959667", "1215984473", "839035965", "899805924", "1019950337", "1157267559", "1190066264", "1219028044", "1213954670", "1216686843", "771879237", "890257768", "815579814", "966532262", "747513404", "776214095", "1217087853", "1218238143", "1217088644", "1125096109", "1205927028", "1214983335", "890736568", "1112877419", "108438038", "1208063099", "572908110", "1218238147", "1217087872", "1218600360", "253959937", "1217088650", "1209983186", "1214974196", "1216204622", "803554963", "1188016998", "1116240990", "1188584181", "961234596", "1217088664", "1217087861", "1147651100", "1126178958", "583783174", "253959934", "337674946", "337674927", "337674936", "549819781", "254322038", "88462845", "1042672099", "253959858", "805541466", "441428903", "441424099", "253959895", "893975396", "98497690", "311145482", "815007148", "1215442088", "1184440605", "138273452", "337674933", "563528988", "1079462761", "847410914", "847410914", "193490625", "953638369", "1136527201", "619759322", "1135723509", "526728712", "976362212", "777635435", "844012935", "974280850", "1214581241", "1214581239", "1212548639", "563528992", "954276177", "777635437", "253959930", "545602684", "975619077", "253959888", "881219855", "981591144", "1027223030", "337674949")

  override def run(ss: SparkSession, config: FeatureIdParserConfig): Unit = {
    val sc = ss.sparkContext
    val osmDF = ss.read.format("osm.pbf").load(config.input.path).repartition(2000)

    val nodesDF = osmDF.select(col("id").as("node_id"), col("latitude"), col("longitude"))
      .filter(col("type") === OsmSqlEntity.ENTITY_TYPE_NODE)

    // Filter out non roads, as well as roads with "area" tag as those are describers of areas surrounded by roads, such as squares.
    val waysDF = osmDF.select("id", "type", "nodes", "tags")
      .filter(col("type") === OsmSqlEntity.ENTITY_TYPE_WAY)
      .filter(col("tags").getItem("highway").isNotNull)
      .filter(col("tags").getItem("area") =!= "yes" || col("tags").getItem("area").isNull)

    val explodedDF = waysDF
      .selectExpr("*", "posexplode(nodes) as (pos, way_node_id)")
      .drop("nodes")

    val joinedDF = explodedDF.join(nodesDF, explodedDF("way_node_id") === nodesDF("node_id"))
      .orderBy("id", "pos")

    val aggregatedDF = joinedDF
      .groupBy("id")
      .agg(collect_list(struct("latitude", "longitude", "pos")).as("coordinates"), collect_list("tags").as("tags"))

    val roads = aggregatedDF.rdd.flatMap { row =>
      val id = row.getAs[Long]("id").toString
      if (IGNORE_LIST.contains(id)) None
      else {
        val tags = row.getAs[Seq[String]]("tags").headOption.getOrElse(Map()).asInstanceOf[Map[String, String]]
        val coordinatesWrappedArray = row.getAs[mutable.WrappedArray[Row]]("coordinates")
        val coordinates = coordinatesWrappedArray
          .map(row => Coordinate(row.getAs[Double]("latitude"), row.getAs[Double]("longitude"), row.getAs[Int]("pos")))
          .sortBy(_.index)
          .map(roundCoordinate)
        Some(RoadSegment(id, coordinates, tags))
      }
    }

    // Computes the composite hash with geohash, elevation, length and angle
    val hashRDD = computeHash(roads, 6, 64, sc)
    logger.info(s"Total number of matching roads in dataset: ${hashRDD.count()}")

    val tileToIndexRDD = geoHashTileToIndex(roads)

    // Replace the current 28 head bits from geohash with the computed index in tileToIndexRDD
    val hashRDDKeyed = hashRDD.map { case (key, value) => (key.take(28), (key, value)) }
    val groupedRDD = hashRDDKeyed.cogroup(tileToIndexRDD)
    val joinedRDD = groupedRDD.flatMap { case (_, (rdd1Values, rdd2Values)) =>
      for {
        (key, value1) <- rdd1Values
        value2 <- rdd2Values
      } yield (key, value1, value2)
    }
    val finalHashRDD = joinedRDD.map {
      case (currHash, roads, newHash) => (newHash ++ currHash.drop(28), roads)
    }.groupByKey.map { case (key, values) =>
      (key, values.flatten.toSeq)
    }

    // Collect stats and log them. Collect to driver in order to have logs available on driver.
    val duplicatesByHash = hashRDD.filter { case (_, roads) => roads.size > 1 }
    val collisions = duplicatesByHash.filter { case (_, roads) =>
      roads.map(_.coordinates.toSet).distinct.size != 1
    }.collect().toMap
    val elevated = duplicatesByHash.filter { case (id, roads) =>
      roads.map(_.tags).exists(tags => tags.keySet.intersect(ELEVATION_TAGS).nonEmpty) &&
        !collisions.contains(id)
    }.collect().toMap
    val duplicates = duplicatesByHash.collect().toMap
    logger.info(s"Total duplicates: ${duplicates.size} with total collisions: ${collisions.size}")
    duplicates.foreach(value => logger.info(s"Duplicates: ${value._2.map(_.name)}"))
    collisions.foreach(value => logger.info(s"Collision: ${value._2.map(_.name)}"))
    elevated.foreach(value => logger.info(s"elevated: ${value._2.map(_.name)}"))
  }

  // Helper method in case parsing through json is preferred,
  // for example manual extract of box through OSM extract API
  private def parseJsonRoads(sc: SparkContext): RDD[RoadSegment] = {

    val filePath = "src/test/resources/input.json"
    val jsonString = sc.textFile(filePath).collect().mkString

    val json = Json.parse(jsonString)
    val features = (json \ "features").as[JsArray].value

    val roadFeaturesRDD = sc.parallelize(features).filter { feature =>
      (feature \ "properties" \ "highway").isDefined &&
        (feature \ "id").as[String].contains("way") &&
        (feature \ "geometry" \ "type").as[String] != "Polygon"
    }

    val featureIdCoordinatesRDD = roadFeaturesRDD.map { feature =>
      val id = (feature \ "id").as[String]
      val coordinates = (feature \ "geometry" \ "coordinates").as[JsArray].value.collect {
        case JsArray(Seq(JsNumber(x), JsNumber(y))) => Coordinate(y.toFloat, x.toFloat, 0)
      }
      val tags = (feature \ "properties").as[Map[String, String]]
      RoadSegment(id, coordinates, tags)
    }
    featureIdCoordinatesRDD
  }
}
