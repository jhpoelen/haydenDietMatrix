elton interactions | cut -f1,2,8,9,10 | sort | uniq | gzip > interactions.tsv.gz
zcat interactions.tsv.gz | cut -f3 | sort | uniq > interactionLabel.tsv
zcat interactions.tsv.gz | grep -P "(\teatenBy\t|\tpreyedUponBy\t)" | awk -F '\t' '{ print $4 "\t" $5 "\t" $1 "\t" $2 }' | gzip > interactionsPredPrey.tsv.gz 
zcat interactions.tsv.gz | grep -P "(\teats\t|\tpreysOn\t)" | cut -f1,2,4,5 | gzip >> interactionsPredPrey.tsv.gz
zcat interactionsPredPrey.tsv.gz | nomer append | grep SAME_AS | cut -f3,4,6,7 | gzip > interactionsPreyPred.tsv.gz
nomer properties | grep -v "nomer.append.schema.output" > my.properties
echo 'nomer.append.schema.output=[{"column":0,"type":"path.order.id"},{"column": 1,"type":"path.order.name"},{"column": 2,"type":"path.order"}]' >> my.properties
zcat interactionsPreyPred.tsv.gz | nomer append --properties=my.properties | grep SAME_AS | awk -F '\t' '{ print $3 "\t" $4 "\t" $1 "\t" $2 "\t" $6 "\t" $7}' | grep -P "^FBC:FB" | grep -v -P "\t\t$" | gzip > fbPredPreyOrder.tsv.gz
zcat fbPredPreyOrder.tsv.gz | cut -f4,6 | sort | uniq -c | sort -n -r > fbPredPreyOrderPreyFrequency.tsv
