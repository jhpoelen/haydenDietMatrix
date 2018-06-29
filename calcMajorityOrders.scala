import spark.implicits._
import org.apache.spark.sql.SaveMode
val predPrey = spark.read.option("delimiter","\t").option("header", "false").csv("fbPredPreyOrder.tsv.gz")

case class Term(id: Option[String], name: Option[String])
case class PredPrey(pred: Term, prey: Term, order: Term)

// get prey id,name pairs with order 
val predPreyDs = predPrey.as[(Option[String], Option[String], Option[String], Option[String], Option[String], Option[String])].map(r => PredPrey(pred = Term(id=r._1, name=r._2), prey=Term(id=r._3,name=r._4), order=Term(id=r._5, name=r._6)))

val predPreyWithPreyOrdersDs = predPreyDs.filter(_.order.id.isDefined).distinct

val preyWithOrder = predPreyWithPreyOrdersDs.map(x => ((x.prey.id, x.prey.name, x.order.name), 1))

// sometimes species match to different (hopefully similar) orders: in this case, the order with the same name is selected. If there's a split vote, the order that is first alphabetically is selected. The latter is a bit arbritary, but necessary to ensure reproducibility.

val distinctMajorityOrders = preyWithOrder.rdd.reduceByKey( _ + _) .map( x => ((x._1._1, x._1._2),( x._2, x._1._3))).reduceByKey( (agg, v) => { if (agg._1 > v._1) agg else {if (v._1 == agg._1 && v._2.getOrElse("") > agg._2.getOrElse("")) agg else v } }).map(_._2._2).distinct

val distinctOrders = predPreyWithPreyOrdersDs.map(_.order.name).distinct
val distinctMinorityOrders = distinctOrders.rdd.subtract(distinctMajorityOrders)

distinctMajorityOrders.toDS.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("majorityOrders")
distinctMinorityOrders.toDS.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("minorityOrders")

val majorOrders = sc.broadcast(distinctMajorityOrders.collect)

val predPreyMajorityOrders = predPreyWithPreyOrdersDs.filter(x => majorOrders.value.contains(x.order.name))

val predPreyMajorityOrdersOnly = predPreyMajorityOrders.map(x => (x.pred.id, x.pred.name, x.order.name)).distinct
val predPreyMajorityOrderCount = predPreyMajorityOrdersOnly.map(x => ((x.pred.id, x.pred.name), 1)).rdd.reduceByKey(_ + _).map(x => (x._1._1, x._1._2, x._2, majorOrders.value.flatten.length))
predPreyMajorityOrdersOnly.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("fbPredPreyMajorityOrder")
predPreyMajorityOrderCount.toDS.write.option("delimiter", "\t").option("header", "false").mode(SaveMode.Overwrite).csv("fbPredPreyMajorityOrderCount")

