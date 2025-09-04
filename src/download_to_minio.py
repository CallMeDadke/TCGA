"""
Download TCGA TSV files from Xena portal and upload to MinIO.
Handles gzip decompression and progress tracking.
"""

import os
import gzip
import shutil
import tempfile
from typing import Dict, Optional
import requests
from tqdm import tqdm
from config import CFG
from xena_scrape import scrape_cohort_urls, verify_url_availability
from minio_utils import get_minio_client, ensure_bucket_exists, upload_file, object_exists

def download_with_progress(url: str, file_path: str, chunk_size: int = 8192) -> bool:
    """Download file with progress bar."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(file_path, 'wb') as file:
            with tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                desc=os.path.basename(file_path)
            ) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)
                        pbar.update(len(chunk))
        
        print(f"[OK] Downloaded: {file_path}")
        return True
        
    except requests.RequestException as e:
        print(f"[FAIL] Download failed for {url}: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error downloading {url}: {e}")
        return False

def decompress_gzip(gzip_path: str, output_path: str) -> bool:
    """Decompress gzip file."""
    try:
        with gzip.open(gzip_path, 'rb') as gz_file:
            with open(output_path, 'wb') as out_file:
                shutil.copyfileobj(gz_file, out_file)
        
        print(f"[OK] Decompressed: {output_path}")
        return True
        
    except Exception as e:
        print(f"[FAIL] Decompression failed: {e}")
        return False

def upload_to_minio(file_path: str, cohort: str, client=None) -> bool:
    """Upload file to MinIO with proper bucket structure."""
    if client is None:
        client = get_minio_client()
    
    bucket_name = CFG['minio']['bucket']
    
    # Create object name: tcga/<cohort>/raw/<filename>
    filename = os.path.basename(file_path)
    object_name = f"tcga/{cohort}/raw/{filename}"
    
    try:
        ensure_bucket_exists(client, bucket_name)
        
        # Check if file already exists
        if object_exists(object_name, bucket_name, client):
            print(f"[SKIP] File already exists in MinIO: {object_name}")
            return True
        
        # Upload file
        client.fput_object(bucket_name, object_name, file_path)
        
        file_size = os.path.getsize(file_path)
        size_mb = file_size / (1024 * 1024)
        print(f"[OK] Uploaded to MinIO: {object_name} ({size_mb:.1f} MB)")
        return True
        
    except Exception as e:
        print(f"[FAIL] MinIO upload failed: {e}")
        return False

def process_cohort(cohort: str, url: str, temp_dir: str, client=None) -> bool:
    """Download and process a single cohort."""
    print(f"\nProcessing cohort: {cohort}")
    print(f"URL: {url}")
    
    # Verify URL first
    if not verify_url_availability(url):
        print(f"[SKIP] Skipping {cohort}: URL not accessible")
        return False
    
    # Determine file paths
    is_gzipped = url.endswith('.gz')
    downloaded_filename = f"TCGA-{cohort}.{'tsv.gz' if is_gzipped else 'tsv'}"
    downloaded_path = os.path.join(temp_dir, downloaded_filename)
    
    # Download file
    print(f"Downloading {cohort}...")
    if not download_with_progress(url, downloaded_path):
        return False
    
    # Handle gzip decompression
    final_file_path = downloaded_path
    if is_gzipped:
        decompressed_filename = f"TCGA-{cohort}.tsv"
        decompressed_path = os.path.join(temp_dir, decompressed_filename)
        
        print(f"Decompressing {cohort}...")
        if decompress_gzip(downloaded_path, decompressed_path):
            final_file_path = decompressed_path
            # Remove compressed file to save space
            os.remove(downloaded_path)
        else:
            print(f"[FAIL] Decompression failed for {cohort}")
            return False
    
    # Upload to MinIO
    print(f"Uploading {cohort} to MinIO...")
    success = upload_to_minio(final_file_path, cohort, client)
    
    # Cleanup temporary file
    if os.path.exists(final_file_path):
        os.remove(final_file_path)
    
    return success

def download_all_cohorts(cohorts: Optional[list] = None, skip_existing: bool = True) -> Dict[str, bool]:
    """Download all configured cohorts to MinIO."""
    if cohorts is None:
        cohorts = CFG['tcga']['cohorts']
    
    print("TCGA Data Download to MinIO")
    print("=" * 40)
    print(f"Cohorts: {cohorts}")
    print(f"Target bucket: {CFG['minio']['bucket']}")
    print(f"Skip existing: {skip_existing}")
    
    # Initialize MinIO client
    client = get_minio_client()
    
    # Discover URLs
    print(f"\nDiscovering download URLs...")
    cohort_urls = scrape_cohort_urls(cohorts)
    
    if not cohort_urls:
        print("[FAIL] No download URLs found!")
        return {}
    
    print(f"Found URLs for {len(cohort_urls)} cohorts")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory(prefix="tcga_download_") as temp_dir:
        print(f"\nUsing temporary directory: {temp_dir}")
        
        results = {}
        successful = 0
        
        for cohort, url in cohort_urls.items():
            # Check if already exists in MinIO
            if skip_existing:
                object_name = f"tcga/{cohort}/raw/TCGA-{cohort}.tsv"
                if object_exists(object_name, CFG['minio']['bucket'], client):
                    print(f"[SKIP] Skipping {cohort}: already exists in MinIO")
                    results[cohort] = True
                    successful += 1
                    continue
            
            # Process cohort
            success = process_cohort(cohort, url, temp_dir, client)
            results[cohort] = success
            
            if success:
                successful += 1
                print(f"[OK] {cohort} completed successfully")
            else:
                print(f"[FAIL] {cohort} failed")
    
    # Summary
    print(f"\n" + "=" * 40)
    print(f"Download Summary:")
    print(f"Successful: {successful}/{len(cohorts)} cohorts")
    
    print(f"\nResults by cohort:")
    for cohort, success in results.items():
        status = "[OK]" if success else "[FAIL]"
        print(f"  {status} {cohort}")
    
    return results

def verify_minio_uploads(cohorts: Optional[list] = None) -> Dict[str, bool]:
    """Verify that files were successfully uploaded to MinIO."""
    if cohorts is None:
        cohorts = CFG['tcga']['cohorts']
    
    print(f"\nVerifying MinIO uploads...")
    print("-" * 40)
    
    client = get_minio_client()
    bucket_name = CFG['minio']['bucket']
    verification_results = {}
    
    for cohort in cohorts:
        object_name = f"tcga/{cohort}/raw/TCGA-{cohort}.tsv"
        exists = object_exists(object_name, bucket_name, client)
        verification_results[cohort] = exists
        
        status = "[OK]" if exists else "[FAIL]"
        print(f"  {status} {cohort}: {object_name}")
    
    successful_uploads = sum(verification_results.values())
    print(f"\nVerification complete: {successful_uploads}/{len(cohorts)} files found in MinIO")
    
    return verification_results

def main():
    """Main function to download cohorts."""
    # Get cohorts from configuration
    cohorts = CFG['tcga']['cohorts']
    
    # Download all cohorts
    results = download_all_cohorts(cohorts)
    
    # Verify uploads
    verification = verify_minio_uploads(cohorts)
    
    # Final status
    successful_downloads = sum(results.values())
    successful_uploads = sum(verification.values())
    
    print(f"\n[FINAL STATUS]:")
    print(f"   Downloads: {successful_downloads}/{len(cohorts)} successful")
    print(f"   MinIO uploads: {successful_uploads}/{len(cohorts)} verified")
    
    if successful_uploads == len(cohorts):
        print("[SUCCESS] All cohorts successfully downloaded and stored in MinIO!")
        return True
    else:
        print("[WARNING] Some cohorts failed. Check logs above for details.")
        return False

if __name__ == "__main__":
    main()