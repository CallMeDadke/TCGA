"""
Xena portal web scraping for TCGA cohort URL discovery.
Finds download URLs for IlluminaHiSeq RNASeqV2 data files.
"""

import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import re
from config import CFG

# TCGA Cancer Cohorts with exact download URLs
CANCER_COHORTS = [
    {
      "Name": "TCGA Acute Myeloid Leukemia (LAML)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LAML.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Adrenocortical Cancer (ACC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.ACC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Bile Duct Cancer (CHOL)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.CHOL.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Bladder Cancer (BLCA)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.BLCA.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Breast Cancer (BRCA)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.BRCA.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Cervical Cancer (CESC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.CESC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Endometrioid Cancer (UCEC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.UCEC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Esophageal Cancer (ESCA)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.ESCA.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Glioblastoma (GBM)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.GBM.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Head and Neck Cancer (HNSC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.HNSC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Kidney Chromophobe (KICH)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.KICH.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Kidney Clear Cell Carcinoma (KIRC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.KIRC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Kidney Papillary Cell Carcinoma (KIRP)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.KIRP.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Large B-cell Lymphoma (DLBC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.DLBC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Liver Cancer (LIHC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LIHC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Lower Grade Glioma (LGG)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LGG.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Lung Adenocarcinoma (LUAD)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LUAD.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Lung Cancer (LUNG)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LUNG.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Lung Squamous Cell Carcinoma (LUSC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LUSC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Melanoma (SKCM)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.SKCM.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Mesothelioma (MESO)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.MESO.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Ocular melanomas (UVM)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.UVM.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Ovarian Cancer (OV)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.OV.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Pancreatic Cancer (PAAD)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.PAAD.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Pheochromocytoma & Paraganglioma (PCPG)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.PCPG.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Prostate Cancer (PRAD)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.PRAD.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Rectal Cancer (READ)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.READ.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Sarcoma (SARC)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.SARC.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Stomach Cancer (STAD)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.STAD.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Testicular Cancer (TGCT)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.TGCT.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Thymoma (THYM)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.THYM.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Thyroid Cancer (THCA)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.THCA.sampleMap%2FHiSeqV2_PANCAN.gz"
    },
    {
      "Name": "TCGA Uterine Carcinosarcoma (UCS)",
      "Url": "https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.UCS.sampleMap%2FHiSeqV2_PANCAN.gz"
    }
]


def get_cohort_code_from_name(name: str) -> Optional[str]:
    """Extract cohort code from full name (e.g. 'TCGA Breast Cancer (BRCA)' -> 'BRCA')."""
    match = re.search(r'\(([A-Z]+)\)', name)
    return match.group(1) if match else None

def get_cohort_info_by_code(cohort_code: str) -> Optional[Dict]:
    """Get cohort info by code from CANCER_COHORTS list."""
    for cohort in CANCER_COHORTS:
        if get_cohort_code_from_name(cohort["Name"]) == cohort_code:
            return cohort
    return None

def get_available_cohort_codes() -> List[str]:
    """Get list of available cohort codes from CANCER_COHORTS."""
    codes = []
    for cohort in CANCER_COHORTS:
        code = get_cohort_code_from_name(cohort["Name"])
        if code:
            codes.append(code)
    return sorted(codes)

def get_cohort_download_url(cohort_code: str) -> Optional[str]:
    """Get download URL for specific cohort code from CANCER_COHORTS."""
    cohort_info = get_cohort_info_by_code(cohort_code)
    if cohort_info:
        print(f"Found URL for {cohort_code}: {cohort_info['Url']}")
        return cohort_info["Url"]
    else:
        print(f"[WARN] No URL found for cohort {cohort_code}")
        return None

def scrape_cohort_urls(cohorts: List[str] = None) -> Dict[str, str]:
    """Get download URLs for TCGA cohorts from CANCER_COHORTS dictionary."""
    if cohorts is None:
        cohorts = CFG['tcga']['cohorts']
    
    print(f"Getting download URLs for cohorts: {cohorts}")
    cohort_urls = {}
    
    for cohort in cohorts:
        print(f"Looking up {cohort}...")
        
        # Get URL from CANCER_COHORTS dictionary
        url = get_cohort_download_url(cohort)
        if url:
            cohort_urls[cohort] = url
        else:
            print(f"[WARN] No URL found for cohort {cohort}")
    
    return cohort_urls

def verify_url_availability(url: str, timeout: int = 10) -> bool:
    """Verify that a URL is accessible and returns a valid file."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        
        if response.status_code == 200:
            # Check content type and size
            content_type = response.headers.get('content-type', '').lower()
            content_length = response.headers.get('content-length')
            
            is_valid = (
                any(ct in content_type for ct in ['gzip', 'octet-stream', 'text']) and
                (content_length is None or int(content_length) > 1000)  # At least 1KB
            )
            
            if is_valid:
                size_mb = int(content_length) / (1024*1024) if content_length else "Unknown"
                print(f"[OK] URL verified: {url} (Size: {size_mb}MB)")
                return True
            else:
                print(f"[FAIL] Invalid content for URL: {url}")
                return False
        else:
            print(f"[FAIL] URL not accessible: {url} (Status: {response.status_code})")
            return False
            
    except requests.RequestException as e:
        print(f"[ERROR] Error verifying URL {url}: {e}")
        return False

def get_available_cohorts() -> List[str]:
    """Get list of TCGA cohorts that have available download URLs."""
    print("Getting available TCGA cohorts from CANCER_COHORTS...")
    
    available_cohorts = get_available_cohort_codes()
    print(f"Found {len(available_cohorts)} cohorts: {available_cohorts}")
    
    return available_cohorts

def main():
    """Main function to test URL discovery."""
    print("TCGA Xena URL Discovery")
    print("=" * 40)
    
    # Show available cohorts
    available = get_available_cohorts()
    print(f"Available cohorts: {len(available)}")
    
    # Get configured cohorts
    cohorts = CFG['tcga']['cohorts']
    print(f"Configured cohorts: {cohorts}")
    
    # Get URLs for configured cohorts
    urls = scrape_cohort_urls(cohorts)
    
    print(f"\nFound URLs:")
    print("-" * 40)
    for cohort, url in urls.items():
        print(f"{cohort}: {url}")
        
    # Verify URLs
    print(f"\nVerifying URLs...")
    print("-" * 40)
    for cohort, url in urls.items():
        verify_url_availability(url)
    
    print(f"\nURL discovery complete. Found {len(urls)}/{len(cohorts)} cohorts.")

if __name__ == "__main__":
    main()