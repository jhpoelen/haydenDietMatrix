#!/bin/bash
#
# script assumes that https://github.com/globalbioticinteractions/elton , https://github.com/globalbioticinteractions/nomer
# and apache spark are available via elton, nomer, and spark-shell aliases respectively.
#

set -xe 

date --iso-8601=seconds
uname -a
java -version
elton version
nomer version
spark-shell --version < /dev/null

function download_interactions_archive() {
  # download the dataset cache (>20GB uncompressed)
  curl -L https://zenodo.org/record/2007419/files/elton-datasets.tar.gz | tar xfz - 

}

function generate_interaction_table() {
  elton interactions | cut -f2,3,13,15,16 | gzip > interactions_dups.tsv.gz
  zcat interactions_dups.tsv.gz | sort | uniq | gzip > interactions.tsv.gz
}

function generate_pred_prey_table() {
  zcat interactions.tsv.gz | cut -f3 | sort | uniq > interactionLabel.tsv
  zcat interactions.tsv.gz | grep -P "(\teatenBy\t|\tpreyedUponBy\t)" | awk -F '\t' '{ print $4 "\t" $5 "\t" $1 "\t" $2 }' | gzip > interactionsPredPrey.tsv.gz 
  zcat interactions.tsv.gz | grep -P "(\teats\t|\tpreysOn\t)" | cut -f1,2,4,5 | gzip >> interactionsPredPrey.tsv.gz
}

function resolve_predator_names() {
  zcat interactionsPredPrey.tsv.gz | nomer append --properties=predator.properties | grep SAME_AS | cut -f3,4,6,7 | gzip > interactionsPreyPred.tsv.gz
}

function map_prey_to_rank() {
  RANK=$1
  # map to prey rank
  zcat interactionsPreyPred.tsv.gz | grep -P ".*\t.*\tFBC:FB" | nomer append --properties=prey${RANK}.properties | grep SAME_AS | grep -v -P "\t$" | grep -v -P "\t\t[a-z]+$" | gzip > fbPreyPredSameAsWith${RANK}.tsv.gz
  cp fbPreyPredSameAsWith${RANK}.tsv.gz fbPreyPredSameAsWithRank.tsv.gz
  zcat interactionsPreyPred.tsv.gz | grep -P ".*\t.*\tFBC:FB" | nomer append --properties=prey${RANK}.properties | grep -v SAME_AS | gzip > fbPreyPredNotSameAsWith${RANK}.tsv.gz
  cp fbPreyPredNotSameAsWith${RANK}.tsv.gz fbPreyPredNotSameAsWithRank.tsv.gz
  # remove likely homonyms
  zcat fbPreyPredSameAsWith${RANK}.tsv.gz | awk -F '\t' '{ print $1 "\t" $2 "\t" $6 "\t" $7 }' | sort | uniq | gzip > fbPreyMap.tsv.gz


  cat removeLikelyHomonyms.scala | spark-shell
  cat fbPreyLikelyHomonyms/*.csv | sort | uniq > fbPreyLikelyHomonymsWith${RANK}.tsv
  cat fbPredPreySameAsWithRankNoHomonyms/*.csv | grep -v -P "\t\t$" | sort | uniq | gzip > fbPredPreySameAsWith${RANK}NoHomonyms.tsv.gz

  zcat fbPredPreySameAsWith${RANK}NoHomonyms.tsv.gz | awk -F '\t' '{ print $1 "\t" $2 "\t" $3 "\t" $4 "\t" $5 "\t" $6 }' | sort | uniq | gzip > fbPredPrey${RANK}Unmapped.tsv.gz

  zcat fbPredPrey${RANK}Unmapped.tsv.gz | sed -f map${RANK}.sed | sort | uniq | gzip > fbPredPrey${RANK}.tsv.gz
  cp fbPredPrey${RANK}.tsv.gz fbPredPreyRank.tsv.gz
  zcat fbPredPrey${RANK}.tsv.gz | cut -f4,6 | sort | uniq -c | sort -n -r > fbPredPrey${RANK}PreyFrequency.tsv

  # calc majority orders

  cat calcMajorityRank.scala | spark-shell
  cat majorityRank/*.csv | sort | uniq > majority${RANK}.tsv
  cat minorityRank/*.csv | sort | uniq > minority${RANK}.tsv
  cat fbPredPreyMajorityRank/*.csv | sort | uniq > fbPredPreyMajority${RANK}.tsv
  cat fbPredPreyMajorityRankCount/*.csv | sort | uniq > fbPredPreyMajority${RANK}Count.tsv
}

function map_prey_to_path() {
  zcat interactionsPreyPred.tsv.gz | grep -P ".*\t.*\tFBC:FB" | nomer append --properties=preyPath.properties | grep SAME_AS | gzip > fbPreyPredSameAsWithFullHierarchy.tsv.gz
}

download_interactions_archive
generate_interaction_table
generate_pred_prey_table
resolve_predator_names
map_prey_to_rank Order
map_prey_to_rank Class
map_prey_to_path
