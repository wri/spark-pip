#!/usr/bin/env bash
rm -rf /tmp/output
rm -r data/output/
spark-submit\
 target/spark-pip-0.3.jar\
 local.properties

./read_local_output.py
