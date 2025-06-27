#!/bin/bash

spark-submit \
  --master local[*] \
  --deploy-mode client \
  spark_app/main.py
