# UrgenceNet.ma — Jalon 1

Ce dépôt couvre les Parties 1, 2 et 3 du mini-projet EMSI. Docker Compose démarre Kafka en mode KRaft, Spark Standalone (un Master et deux Workers), MinIO et Jupyter. Le batch lit le JSONL de Bronze dans MinIO, calcule les indicateurs en RDD et Spark SQL, puis écrit et relit le Parquet Silver.

## Prérequis

Docker Desktop en mode conteneurs Linux ou Docker Engine avec Compose v2, accès aux registres Docker, à GitHub Releases, à PyPI et à Maven Central. Prévoir environ 8 Go de mémoire pour Docker. Versions configurées : Spark/PySpark 3.5.3, Scala 2.12 et Python 3 de l'image Spark, Kafka 3.8.1, Jupyter Notebook 7.3.2, Hadoop AWS 3.3.4, AWS SDK 1.12.262, MinIO et mc (versions fixées dans Dockerfile.minio, binaires officiels téléchargés depuis leurs releases GitHub avec vérification SHA-256). Les identifiants MinIO du Compose sont réservés à la démonstration locale et les interfaces publiées écoutent uniquement sur localhost.

## Exécution

Ouvrir un terminal à la racine du dépôt.

```text
docker compose up -d --build
docker compose ps -a
docker compose logs kafka-init minio-init
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic urgences
docker compose exec jupyter python3 cluster.py
docker compose exec jupyter python3 producer.py --mode file --n 50000
docker compose run --rm --no-deps minio-init "mc alias set local http://minio:9000 urgencenet urgency-demo-local && mc cp /input/urgences_history.jsonl /input/facilities.json local/lake/bronze/history/"
docker compose exec -T jupyter python3 -u batch.py > sortie-batch.txt
docker compose run --rm --no-deps minio-init "mc alias set local http://minio:9000 urgencenet urgency-demo-local && mc ls --recursive local/lake/"
```

Le générateur fourni avec ce dépôt remplace les fichiers de données non joints au sujet. Avec la graine 42, il crée 50 000 lignes pour 12 250 passages, dont 1 000 doublons, et un référentiel JSONL de 40 établissements. Les comptages analytiques sont calculés sur Bronze avant dédoublonnage. La comparaison des délais par auto-jointure utilise les lignes dédoublonnées. Le batch remplace le contenu de son préfixe Silver à chaque exécution. Ne pas lancer deux écritures simultanées sur ce préfixe.

## Interfaces et preuves à joindre

- [Spark Master](http://localhost:8080) : capturer les deux Workers ALIVE, puis consigner la sortie de cluster.py et la description du topic à 3 partitions.
- [MinIO Console](http://localhost:9001) : se connecter avec les identifiants du Compose. Capturer le bucket lake et les partitions city=… sous silver/urgences_history. Conserver aussi la sortie de mc ls.
- [Jupyter](http://localhost:8888) : récupérer le jeton via docker compose exec jupyter jupyter server list. Les scripts s'exécutent également depuis un terminal Jupyter.
- [Spark UI](http://localhost:4040) : accessible durant le batch. Pour retrouver les jobs après exécution, démarrer le History Server dans le conteneur Jupyter avec la commande ci-dessous.
- [Spark History Server](http://localhost:18080) : ouvrir l'application UrgenceNet - Jalon 1. Dans Jobs, sélectionner le job RDD de l'agrégation par ville et le job SQL de la jointure. Capturer le DAG et les stages de chacun. Noter le nombre de stages exécutés et ceux déjà réutilisés. Le groupe BENCH contient les exécutions chronométrées.
- Dans sortie-batch.txt, relever la sortie de explain(True) et les médianes des trois essais après une exécution d'échauffement. Pour la remise, intégrer les captures et les résultats du déploiement au rapport en respectant la limite de 10 pages.

```text
docker compose exec -d -e SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=file:///project/spark-events jupyter /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
```

Les lignes de la sortie du batch incluent le lineage, le top 5, les priorités, les résultats par ville et type, le dédoublonnage, le contrôle par auto-jointure et la relecture de Silver. Après les vérifications, `docker compose down` arrête les services et conserve les volumes.

## Remise

La section 4.1 du sujet demande un lien de dépôt GitHub ou GitLab et un rapport PDF. Elle ne précise ni plateforme de collecte, ni lien de classe, ni e-mail. Utiliser le canal communiqué par l'enseignant pour transmettre le lien et le PDF. Le jalon 1 est daté du 22 novembre 2026.
