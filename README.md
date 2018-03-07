# Moteur de recherche avec Apache Hadoop

## Première étape

Calcul du nombre d’occurrences des mots dans un document. On prend comme clé pour MAP et REDUCE la paire (mot, doc_ID) où doc_ID est l’identifiant du document. Pour réduire le travail des noeuds REDUCE, la suite de traitements suivante sera appliquée :
* Mise en minuscule de tous les mots
* Filtrage des mots inutiles à l’aide de la liste fournie ici
* Suppression des ponctuations et des caractères numériques (par exemple à l’aide d’expressions régulières)
* Suppression des mots de moins de 3 caractères