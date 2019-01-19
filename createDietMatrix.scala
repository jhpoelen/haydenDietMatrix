import spark.implicits._
import org.apache.spark.sql.SaveMode
val predPrey = spark.read.option("delimiter","\t").option("header", "true").csv("fbPredPreySameAsWithFullHierarchy.tsv.gz")

case class Pairwise(predId: String, predName: String, preyId: String, preyName: String, preyPathIds: String, preyPath: String) 

val predPreyDS = predPrey.as[Pairwise]

val preyUnique = predPreyDS.map(x => (x.preyName, List(x.preyId))).distinct

val ambiguousPreyIds = preyUnique.rdd.reduceByKey(_ ::: _).map(y => (y._1, y._2.map(x => (x.split(":").head, x)).groupBy(_._1).map( _._2.map(_._2)).filter(_.length > 1).flatten.toList)).filter(_._2.length > 1).flatMap(_._2).collect.toList


val predPreyHierarchy = predPreyDS.filter(x => !ambiguousPreyIds.contains(x.preyId)).map(x => ((x.predId, x.predName), x.preyPath))

case class Category(name: String, includes: List[String] = List(), excludes: List[String] = List())
case class DietMatrix(id: Int, name: String, preyHierarchy: String)

val cats = List(Category("Anthozoa"), Category("Cnidaria_other", includes = List("Cnidaria"), excludes = List("Anthozoa")), Category("Polychaeta"), Category("Clitellata"), Category("Annelida_other", includes = List("Annelida"), excludes = List("Polychaeta", "Clitellata")), Category("Polyplacophora"), Category("Bivalvia"), Category("Gastropoda"), Category("Cephalopoda"), Category("Mollusca_other", includes = List("Mollusca"), excludes = List("Clitellata", "Bivalvia", "Gastropoda", "Cephalopoda")), Category("Brachiopoda"), Category("Ostracoda"), Category("Maxillopoda"), Category("Malacostraca"), Category("Insecta"), Category("Arthropoda_other", includes = List("Arthropoda"), excludes = List("Brachiopoda", "Ostracoda", "Maxillopoda", "Malacostraca", "Insecta")), Category("Ophiuroidea"), Category("Echinoidea"), Category("Holothuroidea"), Category("Echinodermata_other", includes = List("Echinodermata"), excludes = List("Ophiuroidea", "Echinoidea", "Holothuroidea")), Category("Actinopterygii"), Category("Chondrichthyes"), Category("Amphibia"), Category("Reptilia"), Category("Aves"), Category("Mammalia"), Category("Chordata_other", includes = List("Chordata"), excludes = List("Actinopterygii", "Chondrichthyes", "Amphibia", "Reptilia", "Aves", "Mammalia")), Category("Animalia_other", includes = List("Animalia"), excludes = List("Cnidaria","Annelida","Mollusca","Arthropoda","Echinodermata","Chordata")), Category("Plantae"))

import org.apache.spark.sql._
import org.apache.spark.sql.types._


val funcForCats: String => List[Int] = signature => {
 	cats.map(cat => {
    val sigs = signature.split('|').map(_.trim)
		if (cat.excludes.isEmpty && cat.includes.isEmpty) {
      if (sigs.contains(cat.name)) 1 else 0
    } else {
      if (sigs.intersect(cat.excludes).headOption.isEmpty && sigs.intersect(cat.includes).headOption.nonEmpty) 1 else 0
    }
  })
}

val udfForCats = udf(funcForCats) 

val predPreySignature = predPreyHierarchy.map(x => DietMatrix(x._1._1.replace("FBC:FB:SpecCode:","").toInt, x._1._2, x._2))

val dietMatrix = predPreySignature.withColumn("dietCategories", udfForCats($"preyHierarchy"))

case class Diet(id: Int, name: String, dietCategories: Array[Int])

val dietMatrixReduced = dietMatrix.select("id", "name", "dietCategories").as[Diet].map(d => ((d.id, d.name), d.dietCategories)).rdd.reduceByKey( (agg, v) => agg.zip(v).map { case(x,y) => if (x > y) x else y }).map(x => Diet(x._1._1, x._1._2, x._2)).toDS

val catsToRow: Seq[Int] => Row = dietCategories => {
 	Row.fromSeq(dietCategories.toList ::: List(dietCategories.length, dietCategories.reduce(_ + _))) 
}

val schema = StructType(cats.map(cat => StructField(cat.name, IntegerType, false)) ::: List(StructField("Categories_total", IntegerType, false), StructField("Categories_matched", IntegerType, false)))
val udfForCatsToRow = udf(catsToRow, schema)

val dietMatrixNamed = dietMatrixReduced.withColumn("namedDietCategories", udfForCatsToRow($"dietCategories")) 

dietMatrixNamed.select("id", "name", "namedDietCategories.*").sort("id").coalesce(1).write.option("delimiter", "\t").option("header", "true").mode(SaveMode.Overwrite).csv("dietMatrix")


