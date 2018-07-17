import spark.implicits._
import org.apache.spark.sql.SaveMode

val preyMap = spark.read.option("delimiter","\t").option("header", "false").csv("fbPreyMap.tsv.gz")

// get prey id,name pairs with order
val preyMapDs = preyMap.as[(Option[String], Option[String], Option[String], Option[String])]

val preyByScheme = preyMapDs.filter(_._3.isDefined).map(p => ((p._1, p._2, p._3.get.split(":").head), List((p._3, p._4))))

val preyLikelyHomonymsReduced = preyByScheme.rdd.reduceByKey(_ ::: _).filter(_._2.distinct.length > 1)

val preyLikelyHomonyms = preyLikelyHomonymsReduced.toDS.flatMap(p => p._2.map(h => (p._1._1, p._1._2, h._1, h._2))).distinct.cache()

preyLikelyHomonyms.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("fbPreyLikelyHomonyms")

val predPrey = spark.read.option("delimiter","\t").option("header", "false").csv("fbPreyPredSameAsWithOrder.tsv.gz")

val predPreyDs = predPrey.as[(Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String])]

val homonym: Option[(Option[String], Option[String], Option[String], Option[String])] = None
val preyPredator = preyLikelyHomonyms.map(h => ((h), List(homonym))).union(predPreyDs.map(pp => ((pp._1, pp._2, pp._6, pp._7), List(Some((pp._3, pp._4, pp._9, pp._10))))))

val preyPredatorNoHomonyms = preyPredator.rdd.reduceByKey(_ ::: _).filter(!_._2.contains(None))

preyPredatorNoHomonyms.map{ x =>
  val v = x._2.head.get
  val k = x._1
  (v._1, v._2, k._1, k._2, v._3, v._4)
}.toDS.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("fbPredPreySameAsWithOrderNoHomonyms")
