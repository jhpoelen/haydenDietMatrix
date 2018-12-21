import spark.implicits._
import org.apache.spark.sql.SaveMode
val predPrey = spark.read.option("delimiter","\t").option("header", "false").csv("fbPredPreySameAsWithFullHierarchy.tsv.gz")

val predPreyDS = predPrey.as[(Option[String], Option[String], Option[String], Option[String])]

val predPreyHierarchy = predPreyDS.map(x => ((x._1, x._2), x._4.getOrElse(""))).map(x => (x._1, List(" | ",x._2, " | ").mkString)).rdd.reduceByKey(_ + _)

val predPreyHierarchiesMerged = predPreyHierarchy.map(x => (x._1, x._2.split('|').map(_.trim).filter(_.nonEmpty).distinct.mkString("|")))


case class Category(name: String, includes: List[String] = List(), excludes: List[String] = List())


case class DietMatrix(id: Int, name: String, preySignature: String)

val cats = List(Category("Anthozoa"),
Category("Cnidaria.other", includes = List("Cnidaria"), excludes = List("Anthozoa")),
Category("Polychaeta"),	
Category("Clitellata"),
Category("Annelida.other", includes = List("Annelida"), excludes = List("Polychaeta", "Clitellata")),	
Category("Polyplacophora"),	
Category("Bivalvia"),	
Category("Gastropoda"),	
Category("Cephalopoda"),	
Category("Mollusca.other", includes = List("Mollusca"), excludes = List("Clitellata", "Bivalvia", "Gastropoda", "Cephalopoda")), 
Category("Brachiopoda"),	
Category("Ostracoda"),	
Category("Maxillopoda"),	
Category("Malacostraca"),	
Category("Insecta"),
Category("Arthropoda.other", includes = List("Arthropoda"), excludes = List(	"Brachiopoda", "Ostracoda", "Maxillopoda", "Malacostraca", "Insecta")),
Category("Ophiuroidea"),	
Category("Echinoidea"),	
Category("Holothuroidea"),	
Category("Echinodermata.other", includes = List("Echinodermata"), excludes = List("Ophiuroidea", "Echinoidea", "Holothuroidea")),
Category("Actinopterygii"),	
Category("Chondrichthyes"),	
Category("Amphibia"),	
Category("Reptilia"),	
Category("Aves"),	
Category("Mammalia"),	
Category("Chordata.other", includes = List("Chordata"), excludes = List("Actinopterygii", "Chondrichthyes", "Amphibia", "Reptilia", "Aves", "Mammalia")),
Category("Animalia.other", includes = List("Animalia"), excludes = List("Cnidaria","Annelida","Mollusca","Arthropoda","Echinodermata","Chordata")),
Category("Plantae"))

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val schema = StructType(cats.map(cat => StructField(cat.name, IntegerType, false)) ::: List(StructField("matched.categories", IntegerType, false), StructField("number.of.categories", IntegerType, false)))

val funcForCats: String => Row = signature => {
 	val matches = cats.map(cat => {
    val sigs = signature.split('|')
		if (cat.excludes.isEmpty && cat.includes.isEmpty) {
      if (sigs.contains(cat.name)) 1 else 0
    } else {
      if (sigs.intersect(cat.excludes).headOption.isEmpty && sigs.intersect(cat.includes).headOption.nonEmpty) 1 else 0
    }
  })
	Row.fromSeq(matches ::: List(matches.reduce(_ + _), matches.length))
}

val udfForCats = udf(funcForCats, schema) 

val predPreySignature = predPreyHierarchiesMerged.map(x => DietMatrix(x._1._1.getOrElse("").replace("FBC:FB:SpecCode:","").toInt, x._1._2.getOrElse(""), x._2)).toDS

val dietMatrix = predPreySignature.withColumn("dietCategories", udfForCats($"preySignature"))

dietMatrix.orderBy("id").select("id", "name", "preySignature", "dietCategories.*").coalesce(1).write.option("delimiter", "\t").option("header", "true").mode(SaveMode.Overwrite).csv("fbPredPreyTaxonomicSignature")


