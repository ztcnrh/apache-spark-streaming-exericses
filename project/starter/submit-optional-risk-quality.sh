#!/bin/bash
docker exec -it apache-spark-streaming-exericses-spark-1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 /home/workspace/project/starter/sparkpyoptionalriskquality.py | tee ../../spark/logs/optional-quality.log 