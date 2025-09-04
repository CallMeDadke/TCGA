#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import io
import gzip
from typing import Dict, List, Optional
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from tqdm import tqdm

from src.config import CFG
from src.minio_utils import get_minio_client, get_object_stream

def get_mongo_client() -> MongoClient:
    """Initialize and return MongoDB client."""
    return MongoClient(CFG['mongo']['uri'])

def extract_patient_id(barcode: str) -> str:
    """Extract patient ID from TCGA barcode (TCGA-XX-YYYY-ZZZ... -> TCGA-XX-YYYY)"""
    parts = barcode.split('-')
    if len(parts) >= 3:
        return '-'.join(parts[:3])
    return barcode

def load_clinical_data() -> Dict[str, Dict]:
    """Load and process clinical data from MinIO."""
    print("Loading clinical data from MinIO...")
    
    client = get_minio_client()
    
    path = "data/TCGA_clinical_survival_data.tsv"
    
    clinical_df = None
    try:
        print(f"   Trying: {path}")
        stream = get_object_stream(path, client=client)
        clinical_df = pd.read_csv(stream, sep='\t')
        print(f"Found clinical data at: {path}")
    except Exception as e:
        print(f"   Not found at {path}: {str(e)[:50]}...")
    
    if clinical_df is None:
        # Try loading from local file system as fallback
        local_paths = [
            "data/TCGA_clinical_survival_data.tsv",
            "TCGA_clinical_survival_data.tsv"
        ]
        
        for local_path in local_paths:
            try:
                if os.path.exists(local_path):
                    print(f"   Trying local file: {local_path}")
                    clinical_df = pd.read_csv(local_path, sep='\t')
                    print(f"Found clinical data locally at: {local_path}")
                    break
            except Exception as e:
                print(f"   Error reading local file {local_path}: {str(e)[:50]}...")
                continue
    
    if clinical_df is None:
        print("Clinical data not found in MinIO or locally")
        print("   Please upload TCGA_clinical_survival_data.tsv to MinIO")
        print("   Expected locations: tcga/clinical/, clinical/, data/, or root")
        return {}
    
    try:
        print(f"Loaded clinical data for {len(clinical_df)} records")
        print(f"Available columns: {list(clinical_df.columns)}")
        
        # Convert to dictionary keyed by patient ID
        clinical_dict = {}
        for _, row in clinical_df.iterrows():
            # Extract patient ID from various possible column names
            patient_id = None
            for col_name in ['bcr_patient_barcode', 'sample', 'submitter_id', 'Patient_ID', 'patient_id', 'barcode']:
                if col_name in clinical_df.columns:
                    patient_id = extract_patient_id(str(row[col_name]))
                    break
            
            if not patient_id or patient_id == 'nan':
                # Try first column as fallback
                patient_id = extract_patient_id(str(row.iloc[0]))
            
            if not patient_id or patient_id == 'nan':
                continue
            
            clinical_data = {}
            
            # Map common clinical fields with flexible column matching
            field_mapping = {
                'DSS': ['DSS', 'disease_specific_survival', 'dss'],
                'DSS_time': ['DSS.time', 'disease_specific_survival_time', 'dss_time'],
                'OS': ['OS', 'overall_survival', 'os'],
                'OS_time': ['OS.time', 'overall_survival_time', 'os_time'],
                'clinical_stage': ['ajcc_pathologic_tumor_stage', 'clinical_stage', 'stage', 'pathologic_stage', 'clinical_stage_grouping', 'ajcc_pathologic_stage'],
                'age_at_diagnosis': ['age_at_initial_pathologic_diagnosis', 'age_at_diagnosis', 'age'],
                'gender': ['gender', 'sex'],
                'race': ['race', 'ethnicity'],
                'vital_status': ['vital_status', 'status'],
                'tumor_status': ['tumor_status'],
                'histological_type': ['histological_type', 'histology'],
                'histological_grade': ['histological_grade', 'grade']
            }
            
            for target_field, possible_sources in field_mapping.items():
                for source_field in possible_sources:
                    if source_field in clinical_df.columns:
                        value = row[source_field]
                        if pd.notna(value) and str(value).lower() not in ['nan', 'na', '', 'null']:
                            clinical_data[target_field] = value
                        break
            
            if clinical_data:  # Only add if we have some clinical data
                clinical_dict[patient_id] = clinical_data
        
        print(f"Processed clinical data for {len(clinical_dict)} patients")
        
        # Debug: show sample clinical patient IDs
        if clinical_dict:
            sample_clinical_ids = list(clinical_dict.keys())[:5]
            print(f"Sample clinical patient IDs: {sample_clinical_ids}")
        
        return clinical_dict
        
    except Exception as e:
        print(f"Error loading clinical data: {e}")
        return {}

def update_patients_with_clinical(clinical_data: Dict[str, Dict]) -> int:
    """Update MongoDB documents with clinical data."""
    if not clinical_data:
        print("No clinical data to process")
        return 0
    
    print("Updating MongoDB documents with clinical data...")
    
    mongo_client = get_mongo_client()
    db = mongo_client[CFG['mongo']['db']]
    collection = db[CFG['mongo']['coll']]
    
    updated_count = 0
    batch_operations = []
    batch_size = 1000
    
    try:
        # Get all existing patients
        existing_patients = collection.find({})
        
        # Debug: show sample MongoDB patient IDs
        sample_mongo_patients = list(collection.find({}, {"patient_id": 1}).limit(5))
        mongo_patient_ids = [p.get('patient_id') for p in sample_mongo_patients]
        print(f"Sample MongoDB patient IDs: {mongo_patient_ids}")
        
        # Debug: show sample clinical patient IDs
        clinical_patient_ids = list(clinical_data.keys())[:5]
        print(f"Sample clinical patient IDs: {clinical_patient_ids}")
        
        for doc in tqdm(existing_patients, desc="Processing patients"):
            patient_id = doc.get('patient_id')
            if not patient_id:
                continue
                
            if patient_id in clinical_data:
                # Prepare update operation
                update_doc = {
                    "$set": {
                        "clinical": clinical_data[patient_id]
                    }
                }
                
                batch_operations.append(
                    UpdateOne({"_id": doc["_id"]}, update_doc)
                )
                
                # Execute batch when it reaches the limit
                if len(batch_operations) >= batch_size:
                    result = collection.bulk_write(batch_operations, ordered=False)
                    updated_count += result.modified_count
                    batch_operations = []
        
        # Execute remaining operations
        if batch_operations:
            result = collection.bulk_write(batch_operations, ordered=False)
            updated_count += result.modified_count
        
        print(f"Successfully updated {updated_count} patients with clinical data")
        return updated_count
        
    except PyMongoError as e:
        print(f"MongoDB error during clinical data update: {e}")
        return updated_count
    except Exception as e:
        print(f"Error updating clinical data: {e}")
        return updated_count
    finally:
        mongo_client.close()

def main():
    """Main execution function."""
    print("TCGA Clinical Data Integration")
    print("=" * 40)
    
    # Load clinical data from MinIO
    clinical_data = load_clinical_data()
    
    if not clinical_data:
        print("No clinical data available. Skipping clinical integration.")
        print("To add clinical data:")
        print("   1. Upload TCGA_clinical_survival_data.tsv to MinIO")
        print("   2. Place it in: tcga/clinical/ or clinical/ directory")
        return
    
    # Update MongoDB documents
    updated_count = update_patients_with_clinical(clinical_data)
    
    print(f"\nClinical data integration completed!")
    print(f"Updated {updated_count} patient records")
    
    # Show sample of integrated data
    try:
        mongo_client = get_mongo_client()
        db = mongo_client[CFG['mongo']['db']]
        collection = db[CFG['mongo']['coll']]
        
        sample_doc = collection.find_one({'clinical': {'$exists': True}})
        if sample_doc:
            print(f"\nSample integrated document:")
            print(f"   Patient: {sample_doc.get('patient_id')}")
            print(f"   Cohort: {sample_doc.get('cancer_cohort')}")
            print(f"   Clinical fields: {list(sample_doc.get('clinical', {}).keys())}")
            for field, value in sample_doc.get('clinical', {}).items():
                if len(str(value)) < 50:  # Only show short values
                    print(f"     {field}: {value}")
        
        mongo_client.close()
    except Exception as e:
        print(f"Note: Could not show sample data: {e}")

if __name__ == "__main__":
    main()