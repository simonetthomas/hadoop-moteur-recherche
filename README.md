# Moteur de recherche avec Apache Hadoop

Le but de ce projet est de reproduite le coeur d'un moteur de recherche. Pour ce faire, on découpe le travail en trois étapes Map/Reduce.

## Première étape

Calcul du nombre d’occurrences des mots dans un document. On prend comme clé pour MAP et REDUCE la paire (mot, doc_ID) où doc_ID est l’identifiant du document. Pour réduire le travail des noeuds REDUCE, la suite de traitements suivante sera appliquée :

* Mise en minuscule de tous les mots ;
* Filtrage des mots inutiles (stopwords) à l’aide de la liste fournie ;
* Suppression des ponctuations et des caractères numériques ;
* Suppression des mots de moins de 3 caractères.

## Deuxième étape

Le calcul du nombre total de mots dans chaque document que nous appellerons WordPerDoc. Pour cette tâche, l’entrée sera donc la sortie de la tâche précédente. On doit avoir en sortie de cette tâche un ensemble de paires( (mot, doc_ID), (wordcount, wordperdoc) ).

## Troisième étape

Enfin, la dernière tâche est finalement le calcul du TF-IDF lui-même. Elle prend en entrée la sortie de la tâche 2 et nous aurons en sortie un ensemble de paires ( (mot, doc_ID), TF-IDF) ).