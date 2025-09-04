"""
Transform TCGA TSV data from MinIO to MongoDB.
ETL pipeline for cGAS-STING pathway gene expression analysis.
"""

import pandas as pd
import numpy as np
import gzip
import io
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from tqdm import tqdm

from config import CFG
from minio_utils import get_minio_client, get_object_stream, list_objects

def get_mongo_client() -> MongoClient:
    """Initialize and return MongoDB client."""
    return MongoClient(CFG['mongo']['uri'])

def get_mongo_collection(client: Optional[MongoClient] = None):
    """Get MongoDB collection for gene expression data."""
    if client is None:
        client = get_mongo_client()
    
    db = client[CFG['mongo']['db']]
    collection = db[CFG['mongo']['coll']]
    return collection

def normalize_gene_names(genes: List[str]) -> Dict[str, str]:
    """Normalize gene names and handle synonyms (IL8/CXCL8)."""
    normalized = {}
    
    for gene in genes:
        # Handle IL8/CXCL8 synonym
        if gene == 'IL8' or gene == 'CXCL8':
            normalized[gene] = 'IL8'  # Use IL8 as canonical name
        else:
            normalized[gene] = gene
    
    return normalized

def process_all_genes(df: pd.DataFrame) -> pd.DataFrame:
    """Process dataframe to include ALL available genes."""
    # Get all genes (columns except sample column)
    available_genes = set(df.columns) - {'sample'}
    
    print(f"[INFO] Processing ALL {len(available_genes)} genes available in dataset")
    print(f"[INFO] Sample of genes: {sorted(list(available_genes)[:10])}...")
    
    # Keep all columns (sample + all genes)
    return df.copy()

def extract_patient_id_and_cohort(sample_id: str) -> tuple[str, str]:
    """Extract patient ID and cohort from TCGA sample ID."""
    # TCGA sample format: TCGA-XX-YYYY-ZZZ-AAA-BBB
    # Patient ID: TCGA-XX-YYYY
    # Cohort: XX (e.g., BRCA, LUAD)
    
    parts = sample_id.split('-')
    if len(parts) >= 3 and parts[0] == 'TCGA':
        patient_id = f"{parts[0]}-{parts[1]}-{parts[2]}"
        cohort = parts[1]
        return patient_id, cohort
    else:
        # Fallback: use full sample ID as patient ID
        return sample_id, "UNKNOWN"

def create_mongo_document(row: pd.Series, gene_columns: List[str]) -> Dict[str, Any]:
    """Create MongoDB document from pandas row with ALL genes."""
    sample_id = row['sample']
    patient_id, cohort = extract_patient_id_and_cohort(sample_id)
    
    # Create genes dictionary with ALL available genes
    genes = {}
    for gene in gene_columns:
        value = row[gene]
        
        # Convert numpy types to native Python types for MongoDB
        if pd.isna(value):
            genes[gene] = 0.0  # Replace NaN with 0
        elif isinstance(value, (np.integer, np.floating)):
            genes[gene] = float(value)
        else:
            genes[gene] = value
    
    # Create document with proper ID format for clinical matching
    document_id = f"{cohort}:{patient_id}"
    
    document = {
        '_id': document_id,
        'patient_id': patient_id,
        'sample_id': sample_id,
        'cancer_cohort': cohort,
        'genes': genes,
        'clinical': {}  # Will be populated later by join_clinical.py
    }
    
    return document

def process_transposed_tsv_data(file_stream, cohort: str = None) -> List[Dict[str, Any]]:
    """Process transposed TSV data where genes are rows and samples are columns."""
    print(f"[INFO] Processing transposed TSV data for cohort: {cohort}")
    
    target_genes = set(CFG['cgas_sting_genes'])
    all_documents = []
    
    try:
        # Read the entire TSV file (genes as rows, samples as columns)
        print("[INFO] Reading TSV file...")
        df = pd.read_csv(file_stream, sep='\t', index_col=0, low_memory=False)
        
        print(f"[INFO] Original data shape: {df.shape} (genes x samples)")
        print(f"[INFO] Sample column names: {list(df.columns[:5])}...")
        print(f"[INFO] Sample gene names: {list(df.index[:5])}...")
        
        # Filter for target genes (cGAS-STING pathway)
        available_genes = set(df.index)
        genes_to_keep = available_genes.intersection(target_genes)
        
        # Handle IL8/CXCL8 synonym
        if 'IL8' not in genes_to_keep and 'CXCL8' in available_genes:
            genes_to_keep.add('CXCL8')
            print("[INFO] Using CXCL8 as synonym for IL8")
        
        print(f"[INFO] Found {len(genes_to_keep)} cGAS-STING genes: {sorted(genes_to_keep)}")
        print(f"[INFO] Missing genes: {target_genes - genes_to_keep}")
        
        if not genes_to_keep:
            print("[WARN] No target genes found in dataset")
            return []
        
        # Filter dataframe for target genes
        df_genes = df.loc[list(genes_to_keep)]
        print(f"[INFO] Filtered data shape: {df_genes.shape} (target genes x samples)")
        
        # Transpose the dataframe so samples become rows and genes become columns
        df_transposed = df_genes.transpose()
        print(f"[INFO] Transposed data shape: {df_transposed.shape} (samples x genes)")
        
        # Reset index to make sample IDs a column
        df_transposed = df_transposed.reset_index()
        df_transposed = df_transposed.rename(columns={'index': 'sample'})
        
        print(f"[INFO] Final data shape: {df_transposed.shape}")
        print(f"[INFO] Sample rows: {list(df_transposed['sample'][:5])}")
        print(f"[INFO] Gene columns: {list(df_transposed.columns[1:6])}")
        
        # Filter for target cohort if specified
        if cohort and cohort != "PAN_CANCER":
            cohort_mask = df_transposed['sample'].str.contains(f'TCGA-{cohort}-', na=False)
            df_filtered = df_transposed[cohort_mask]
            
            if df_filtered.empty:
                print(f"[INFO] No samples for cohort {cohort}")
                return []
            
            print(f"[INFO] Filtered to {len(df_filtered)} samples for cohort {cohort}")
        else:
            df_filtered = df_transposed
        
        # Convert to MongoDB documents
        gene_columns = [col for col in df_filtered.columns if col != 'sample']
        print(f"[INFO] Creating MongoDB documents for {len(df_filtered)} samples with {len(gene_columns)} genes...")
        
        for _, row in df_filtered.iterrows():
            doc = create_mongo_document(row, gene_columns)
            all_documents.append(doc)
        
        print(f"[INFO] Created {len(all_documents)} MongoDB documents")
        return all_documents
        
    except Exception as e:
        print(f"[ERROR] Failed to process transposed TSV data: {e}")
        import traceback
        traceback.print_exc()
        return []

def process_tsv_data(tsv_content: bytes, cohort: str = None) -> List[Dict[str, Any]]:
    """Process TSV content and return list of MongoDB documents."""
    print(f"[INFO] Processing TSV data for cohort: {cohort}")
    
    # Handle gzip decompression
    try:
        if tsv_content[:2] == b'\x1f\x8b':  # gzip magic number
            print("[INFO] Decompressing gzip content")
            content = gzip.decompress(tsv_content).decode('utf-8')
        else:
            content = tsv_content.decode('utf-8')
    except Exception as e:
        print(f"[ERROR] Failed to decompress/decode content: {e}")
        return []
    
    # Use transposed processing for TCGA data (genes as rows, samples as columns)
    file_stream = io.StringIO(content)
    
    try:
        all_documents = process_transposed_tsv_data(file_stream, cohort)
        print(f"[INFO] Created total of {len(all_documents)} MongoDB documents")
        return all_documents
        
    except Exception as e:
        print(f"[ERROR] Failed to process TSV data: {e}")
        return []

def insert_documents_to_mongo(documents: List[Dict[str, Any]], collection=None, batch_size: int = 1000) -> int:
    """Insert documents to MongoDB collection in batches for optimal performance."""
    if not documents:
        print("[WARN] No documents to insert")
        return 0
    
    if collection is None:
        collection = get_mongo_collection()
    
    print(f"[INFO] Inserting {len(documents)} documents to MongoDB (batch size: {batch_size})")
    
    total_inserted = 0
    
    # Process documents in batches to handle large datasets efficiently
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(documents) + batch_size - 1) // batch_size
        
        print(f"[INFO] Processing batch {batch_num}/{total_batches} ({len(batch)} documents)")
        
        try:
            # Use upsert for idempotent inserts (replace existing documents)
            from pymongo import UpdateOne
            
            upsert_operations = []
            for doc in batch:
                operation = UpdateOne(
                    {'_id': doc['_id']},
                    {'$set': doc},
                    upsert=True
                )
                upsert_operations.append(operation)
            
            result = collection.bulk_write(upsert_operations, ordered=False)
            batch_inserted = result.upserted_count + result.modified_count
            total_inserted += batch_inserted
            
            print(f"[OK] Batch {batch_num} complete: {batch_inserted} documents upserted")
            
        except PyMongoError as e:
            print(f"[ERROR] Batch {batch_num} failed: {e}")
            continue
    
    print(f"[OK] MongoDB insert complete:")
    print(f"  - Total inserted: {total_inserted}")
    print(f"  - Total processed: {len(documents)}")
    
    return total_inserted

def process_file_from_minio(object_name: str, minio_client=None, mongo_collection=None) -> int:
    """Process a single file from MinIO to MongoDB."""
    print(f"\n[TRANSFORM] Processing file: {object_name}")
    
    if minio_client is None:
        minio_client = get_minio_client()
    
    if mongo_collection is None:
        mongo_collection = get_mongo_collection()
    
    bucket_name = CFG['minio']['bucket']
    
    # Stream and process data directly
    try:
        print(f"[INFO] Streaming data from MinIO: {object_name}")
        response = get_object_stream(object_name, bucket_name, minio_client)
        
        if response is None:
            print(f"[ERROR] Failed to get object from MinIO: {object_name}")
            return 0
        
        # Process data with transposed handling
        total_processed = 0
        
        try:
            # Handle gzip compressed files
            if object_name.endswith('.gz'):
                import gzip
                # For gzip files, we need to read all data first then decompress
                compressed_data = response.read()
                response.close()
                response.release_conn()
                
                decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
                stream = io.StringIO(decompressed_data)
            else:
                # For uncompressed files, read directly 
                content = response.read().decode('utf-8')
                response.close()
                response.release_conn()
                stream = io.StringIO(content)
            
            # Process using the transposed data handler
            documents = process_transposed_tsv_data(stream, cohort="PAN_CANCER")
            
            if documents:
                # Insert all documents
                processed_count = insert_documents_to_mongo(documents, mongo_collection, batch_size=1000)
                total_processed += processed_count
                print(f"[INFO] File processing complete: {len(documents)} documents created, {processed_count} inserted")
            else:
                print("[WARN] No documents created from file")
        
        finally:
            # Response already closed in the try block
            pass
        
        print(f"[OK] File {object_name} processed: {total_processed} documents")
        return total_processed
        
    except Exception as e:
        print(f"[ERROR] Processing failed for file {object_name}: {e}")
        return 0

def transform_all_files_from_minio(minio_client=None, mongo_collection=None) -> Dict[str, int]:
    """Transform all available files from MinIO to MongoDB."""
    print(f"\n[TRANSFORM] Processing all files from MinIO")
    
    if minio_client is None:
        minio_client = get_minio_client()
    
    if mongo_collection is None:
        mongo_collection = get_mongo_collection()
    
    bucket_name = CFG['minio']['bucket']
    
    # List all files in the tcga/ prefix
    print("[INFO] Listing files in MinIO...")
    file_list = list_objects(prefix="tcga/", bucket_name=bucket_name, client=minio_client)
    
    # Filter for TSV and gz files
    tsv_files = [f for f in file_list if f.endswith('.tsv') or f.endswith('.gz')]
    
    print(f"[INFO] Found {len(tsv_files)} TSV/GZ files to process:")
    for f in tsv_files:
        print(f"  - {f}")
    
    results = {}
    total_processed = 0
    
    # Process each file with progress tracking
    for idx, file_path in enumerate(tsv_files, 1):
        print(f"\n[PROGRESS] Processing file {idx}/{len(tsv_files)}: {file_path}")
        processed_count = process_file_from_minio(file_path, minio_client, mongo_collection)
        results[file_path] = processed_count
        total_processed += processed_count
        
        print(f"[INFO] File {idx}/{len(tsv_files)} completed: {processed_count} documents processed")
        print(f"[INFO] Running total: {total_processed} documents")
    
    print(f"[SUMMARY] Total files processed: {len(tsv_files)}")
    print(f"[SUMMARY] Total documents processed: {total_processed}")
    
    return results

def transform_all_files() -> Dict[str, int]:
    """Transform all files from MinIO to MongoDB gene expression collection."""
    print("TCGA Data Transformation to MongoDB")
    print("=" * 40)
    print(f"Processing: ALL patients with cGAS-STING pathway genes")
    print(f"Target collection: {CFG['mongo']['coll']}")
    
    # Initialize connections
    minio_client = get_minio_client()
    mongo_collection = get_mongo_collection()
    
    # Process all available files
    results = transform_all_files_from_minio(minio_client, mongo_collection)
    
    # Summary
    print(f"\n" + "=" * 40)
    print(f"Transformation Summary:")
    successful_files = sum(1 for count in results.values() if count > 0)
    total_documents = sum(results.values())
    print(f"Files processed: {successful_files}/{len(results)} successful")
    print(f"Total documents: {total_documents}")
    
    print(f"\nResults by file:")
    for file_path, doc_count in results.items():
        status = "[OK]" if doc_count > 0 else "[FAIL]"
        print(f"  {status} {file_path}: {doc_count} documents")
    
    return results

def verify_mongo_data() -> Dict[str, Any]:
    """Verify transformed data in MongoDB gene expression collection."""
    print(f"\n[VERIFY] Checking MongoDB {CFG['mongo']['coll']} collection...")
    print("-" * 40)
    
    collection = get_mongo_collection()
    
    try:
        # Overall statistics
        total_docs = collection.count_documents({})
        print(f"Total documents: {total_docs}")
        
        # Per-cohort statistics
        cohort_pipeline = [
            {'$group': {'_id': '$cancer_cohort', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]
        cohort_stats = list(collection.aggregate(cohort_pipeline))
        
        print(f"\nPatients per cohort:")
        cohort_dict = {}
        for stat in cohort_stats[:20]:  # Show top 20 cohorts
            cohort = stat['_id']
            count = stat['count']
            cohort_dict[cohort] = count
            print(f"  {cohort}: {count} patients")
        
        if len(cohort_stats) > 20:
            print(f"  ... and {len(cohort_stats) - 20} more cohorts")
        
        # Sample document
        sample_doc = collection.find_one()
        if sample_doc:
            print(f"\nSample document structure:")
            print(f"  Patient ID: {sample_doc.get('patient_id')}")
            print(f"  Cohort: {sample_doc.get('cancer_cohort')}")
            print(f"  Genes: {len(sample_doc.get('genes', {}))} genes")
            print(f"  Gene names: {list(sample_doc.get('genes', {}).keys())}")
        
        # Gene statistics
        pipeline = [
            {'$project': {'gene_count': {'$size': {'$objectToArray': '$genes'}}}},
            {'$group': {'_id': None, 'avg_genes': {'$avg': '$gene_count'}, 'min_genes': {'$min': '$gene_count'}, 'max_genes': {'$max': '$gene_count'}}}
        ]
        gene_stats = list(collection.aggregate(pipeline))
        if gene_stats:
            stats = gene_stats[0]
            print(f"\nGene statistics:")
            print(f"  Average genes per patient: {stats.get('avg_genes', 0):.1f}")
            print(f"  Min genes: {stats.get('min_genes', 0)}")
            print(f"  Max genes: {stats.get('max_genes', 0)}")
        
        verification_result = {
            'total_documents': total_docs,
            'cohort_stats': cohort_dict,
            'sample_document': sample_doc,
            'gene_stats': gene_stats[0] if gene_stats else {},
            'success': total_docs > 0
        }
        
        print(f"\n[{'OK' if total_docs > 0 else 'FAIL'}] MongoDB verification complete")
        return verification_result
        
    except Exception as e:
        print(f"[ERROR] MongoDB verification failed: {e}")
        return {'success': False, 'error': str(e)}

def main():
    """Main function to transform all files to cohorts collection."""
    # Transform all files
    results = transform_all_files()
    
    # Verify results
    verification = verify_mongo_data()
    
    # Final status
    successful_files = sum(1 for count in results.values() if count > 0)
    total_files = len(results)
    total_documents = sum(results.values())
    
    print(f"\n[FINAL STATUS]:")
    print(f"   Files processed: {successful_files}/{total_files} successful")
    print(f"   Total documents: {total_documents}")
    print(f"   MongoDB documents: {verification.get('total_documents', 0)}")
    
    if successful_files > 0 and verification.get('success'):
        print(f"[SUCCESS] Files successfully transformed to MongoDB {CFG['mongo']['coll']} collection!")
        return True
    else:
        print("[WARNING] Some transformations failed. Check logs above for details.")
        return False

if __name__ == "__main__":
    main()