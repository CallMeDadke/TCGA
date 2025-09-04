#!/bin/bash

export PYTHONPATH=/app/src:$PYTHONPATH

echo "Starting TCGA Pipeline..."
echo "=========================="

echo "Step 1: Downloading data to MinIO..."
python src/download_to_minio.py

echo "Step 2: Transforming data to MongoDB..."
python src/transform_to_mongo.py

echo "Step 3: Adding clinical data..."
python src/join_clinical.py

echo "Step 4: Generating visualizations..."
python src/visualize.py

echo "Pipeline completed!"