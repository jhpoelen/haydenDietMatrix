import spark.implicits._
import org.apache.spark.sql.SaveMode
val predPrey = spark.read.option("delimiter","\t").option("header", "false").csv("fbPredPreyRank.tsv.gz")

case class Term(id: Option[String], name: Option[String])
case class PredPrey(pred: Term, prey: Term, rank: Term)

// get prey id,name pairs with rank 
val predPreyDs = predPrey.as[(Option[String], Option[String], Option[String], Option[String], Option[String], Option[String])].map(r => PredPrey(pred = Term(id=r._1, name=r._2), prey=Term(id=r._3,name=r._4), rank=Term(id=r._5, name=r._6)))

val predPreyWithPreyRankDs = predPreyDs.filter(_.rank.id.isDefined).distinct

val preyWithRank = predPreyWithPreyRankDs.map(x => ((x.prey.id, x.prey.name, x.rank.name), 1))

// sometimes species match to different (hopefully similar) ranks: in this case, the rank with the same name is selected. If there's a split vote, the rank that is first alphabetically is selected. The latter is a bit arbritary, but necessary to ensure reproducibility.

val distinctMajorityRank = preyWithRank.rdd.reduceByKey( _ + _) .map( x => ((x._1._1, x._1._2),( x._2, x._1._3))).reduceByKey( (agg, v) => { if (agg._1 > v._1) agg else {if (v._1 == agg._1 && v._2.getOrElse("") > agg._2.getOrElse("")) agg else v } }).map(_._2._2).distinct

val distinctRank = predPreyWithPreyRankDs.map(_.rank.name).distinct
val distinctMinorityRank = distinctRank.rdd.subtract(distinctMajorityRank)

distinctMajorityRank.toDS.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("majorityRank")
distinctMinorityRank.toDS.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("minorityRank")

val majorRank = sc.broadcast(distinctMajorityRank.collect)

val predPreyMajorityRank = predPreyWithPreyRankDs.filter(x => majorRank.value.contains(x.rank.name))

val predPreyMajorityRankOnly = predPreyMajorityRank.map(x => (x.pred.id, x.pred.name, x.rank.name)).distinct
val predPreyMajorityRankCount = predPreyMajorityRankOnly.map(x => ((x._1, x._2), 1)).rdd.reduceByKey(_ + _).map(x => (x._1._1, x._1._2, x._2, majorRank.value.flatten.length))
predPreyMajorityRankOnly.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("fbPredPreyMajorityRank")
predPreyMajorityRankCount.toDS.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("fbPredPreyMajorityRankCount")

