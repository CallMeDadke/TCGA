"""
Get patient data from MongoDB.
Usage: python src/get_patient.py {patient_id}
"""

import sys
import json
from typing import Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import CFG

def get_mongo_collection():
    """Get MongoDB collection for gene expression data."""
    client = MongoClient(CFG['mongo']['uri'])
    db = client[CFG['mongo']['db']]
    collection = db[CFG['mongo']['coll']]
    return collection

def get_patient_data(patient_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve patient data from MongoDB.
    
    Args:
        patient_id: TCGA patient ID (e.g., TCGA-XX-YYYY)
    
    Returns:
        Patient document or None if not found
    """
    collection = get_mongo_collection()
    
    try:
        # Search by patient_id field
        patient = collection.find_one({"patient_id": patient_id})
        
        if patient:
            return patient
        
        # If not found by patient_id, try searching by _id pattern
        # _id format is "COHORT:PATIENT_ID"
        patient = collection.find_one({"_id": {"$regex": f".*:{patient_id}$"}})
        
        return patient
        
    except PyMongoError as e:
        print(f"[ERROR] MongoDB query failed: {e}")
        return None

def format_patient_output(patient_data: Dict[str, Any]) -> str:
    """Format patient data for readable output."""
    if not patient_data:
        return "Patient not found."
    
    output = []
    output.append("=" * 60)
    output.append(f"PATIENT DATA: {patient_data.get('patient_id', 'Unknown')}")
    output.append("=" * 60)
    
    # Basic information
    output.append(f"Patient ID: {patient_data.get('patient_id', 'N/A')}")
    output.append(f"Sample ID: {patient_data.get('sample_id', 'N/A')}")
    output.append(f"Cancer Cohort: {patient_data.get('cancer_cohort', 'N/A')}")
    output.append(f"Document ID: {patient_data.get('_id', 'N/A')}")
    
    # Gene expression data
    genes = patient_data.get('genes', {})
    if genes:
        output.append(f"\nGENE EXPRESSION DATA ({len(genes)} genes):")
        output.append("-" * 40)
        
        # Sort genes by expression level (descending)
        sorted_genes = sorted(genes.items(), key=lambda x: x[1], reverse=True)
        
        for gene_name, expression_value in sorted_genes:
            output.append(f"{gene_name:15}: {expression_value:>8.3f}")
    else:
        output.append("\nNo gene expression data available.")
    
    # Clinical data
    clinical = patient_data.get('clinical', {})
    if clinical and any(clinical.values()):
        output.append(f"\nCLINICAL DATA:")
        output.append("-" * 40)
        
        for field, value in clinical.items():
            if value is not None and value != "":
                output.append(f"{field:20}: {value}")
    else:
        output.append("\nNo clinical data available.")
    
    return "\n".join(output)

def main():
    """Main function to retrieve and display patient data."""
    if len(sys.argv) != 2:
        print("Usage: python src/get_patient.py {patient_id}")
        print("Example: python src/get_patient.py TCGA-AR-A1AK")
        sys.exit(1)
    
    patient_id = sys.argv[1].strip()
    
    if not patient_id:
        print("Error: Patient ID cannot be empty")
        sys.exit(1)
    
    print(f"Searching for patient: {patient_id}")
    print(f"MongoDB: {CFG['mongo']['db']}.{CFG['mongo']['coll']}")
    print("-" * 60)
    
    # Get patient data
    patient_data = get_patient_data(patient_id)
    
    if patient_data:
        print(format_patient_output(patient_data))
        
        # Option to export as JSON
        try:
            export_choice = input("\nExport as JSON file? (y/n): ").lower().strip()
            if export_choice in ['y', 'yes']:
                filename = f"data/patient_{patient_id.replace('-', '_')}.json"
                try:
                    # Convert ObjectId to string for JSON serialization
                    json_data = json.loads(json.dumps(patient_data, default=str))
                    
                    with open(filename, 'w') as f:
                        json.dump(json_data, f, indent=2)
                    print(f"Patient data exported to: {filename}")
                except Exception as e:
                    print(f"Error exporting JSON: {e}")
        except EOFError:
            # Handle case where input is not available (automated execution)
            pass
    else:
        print(f"Patient '{patient_id}' not found in database.")
        print("\nTip: Make sure the patient ID is correct and data has been loaded.")
        print("Format: TCGA-XX-YYYY (e.g., TCGA-AR-A1AK)")
        sys.exit(1)

if __name__ == "__main__":
    main()