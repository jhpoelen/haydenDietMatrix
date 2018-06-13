elton interactions | cut -f1,2,8,9,10 | sort | uniq | gzip > interactions.tsv.gz
zcat interactions.tsv.gz | cut -f3 | sort | uniq > interactionLabel.tsv
zcat interactions.tsv.gz | grep -P "(\teatenBy\t|\tpreyedUponBy\t)" | awk -F '\t' '{ print $4 "\t" $5 "\t" $1 "\t" $2 }' | gzip > interactionsPredPrey.tsv.gz 
zcat interactions.tsv.gz | grep -P "(\teats\t|\tpreysOn\t)" | cut -f1,2,4,5 | gzip >> interactionsPredPrey.tsv.gz 