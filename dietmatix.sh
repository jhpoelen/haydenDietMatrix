elton interactions | cut -f1,2,8,9,10 | sort | uniq | gzip > interactions.tsv.gz
zcat interactions.tsv.gz | cut -f3 | sort | uniq > interactionLabel.tsv