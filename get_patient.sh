#!/bin/bash

export PYTHONPATH=/app/src:$PYTHONPATH

# Get patient data shell script
# Usage: ./get_patient.sh {patient_id}

if [ $# -eq 0 ]; then
    echo "Usage: ./get_patient.sh {patient_id}"
    echo "Example: ./get_patient.sh TCGA-OR-A5LC"
    exit 1
fi

python src/get_patient.py "$1"