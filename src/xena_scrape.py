"""
Xena portal web scraping for TCGA cohort URL discovery using Selenium.
Dynamically finds download URLs for pancan normalized gene expression data files.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import requests
import re
from typing import Dict, List, Optional
from config import CFG

def setup_chrome_driver(headless: bool = True) -> webdriver.Chrome:
    """Setup Chrome driver with appropriate options for Docker/local environments."""
    options = Options()
    
    # Always use headless in Docker, optional locally
    if headless:
        options.add_argument("--headless=new")
    
    # Docker-friendly Chrome options
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # Additional Docker-specific options
    import os
    if os.path.exists('/.dockerenv'):
        print("[INFO] Running in Docker environment")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
    
    try:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        # Try to use webdriver-manager for automatic ChromeDriver management
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            print("[INFO] Using ChromeDriver from webdriver-manager")
            return driver
        except Exception as e:
            print(f"[WARN] webdriver-manager failed: {e}")
            
        # Fallback to system ChromeDriver
        driver = webdriver.Chrome(options=options)
        print("[INFO] Using system ChromeDriver")
        return driver
        
    except WebDriverException as e:
        print(f"[ERROR] Failed to initialize Chrome driver: {e}")
        print("Solutions:")
        print("1. Install webdriver-manager: pip install webdriver-manager")
        print("2. Or download ChromeDriver manually from: https://chromedriver.chromium.org/")
        print("3. For Docker: rebuild container with updated Dockerfile")
        raise

def extract_cohort_code_from_text(text: str) -> Optional[str]:
    """Extract cohort code from cancer type text."""
    # Look for patterns like "TCGA Breast Cancer (BRCA)" -> "BRCA"
    match = re.search(r'\(([A-Z]+)\)', text)
    if match:
        return match.group(1)
    
    # Fallback: look for common TCGA cohort codes
    tcga_codes = ['ACC', 'BLCA', 'BRCA', 'CESC', 'CHOL', 'COAD', 'DLBC', 'ESCA', 
                  'GBM', 'HNSC', 'KICH', 'KIRC', 'KIRP', 'LAML', 'LGG', 'LIHC', 
                  'LUAD', 'LUSC', 'MESO', 'OV', 'PAAD', 'PCPG', 'PRAD', 'READ', 
                  'SARC', 'SKCM', 'STAD', 'TGCT', 'THCA', 'THYM', 'UCEC', 'UCS', 
                  'UVM', 'LUNG']
    
    for code in tcga_codes:
        if code in text.upper():
            return code
    
    return None

def scrape_cohort_urls_selenium(cohorts: List[str] = None, headless: bool = True, timeout: int = 10) -> Dict[str, str]:
    """
    Scrape TCGA cohort download URLs using Selenium.
    Based on the NEW SCRIPT GUIDE FOR SCRAPING LOGIC in CLAUDE.md
    """
    if cohorts is None:
        cohorts = CFG['tcga']['cohorts']
    
    print(f"Starting Selenium scraping for cohorts: {cohorts}")
    
    driver = None
    cohort_urls = {}
    
    try:
        driver = setup_chrome_driver(headless=headless)
        
        # Navigate to Xena Browser
        xena_url = 'https://xenabrowser.net/datapages/?host=https%3A%2F%2Ftcga.xenahubs.net&removeHub=https%3A%2F%2Fxena.treehouse.gi.ucsc.edu%3A443'
        print(f"[INFO] Navigating to: {xena_url}")
        driver.get(xena_url)
        
        # Wait for page to load
        time.sleep(4)
        
        # Find all cancer type links
        cancer_types = WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.XPATH, "//ul[@class='Datapages-module__list___2yM9o']//li/a"))
        )
        
        print(f"[INFO] Found {len(cancer_types)} cancer types on page")
        
        # Process each cancer type
        for i in range(len(cancer_types)):
            try:
                # Re-find elements to avoid stale reference
                WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, "//ul[@class='Datapages-module__list___2yM9o']"))
                )
                cancer_types = driver.find_elements(By.XPATH, "//ul[@class='Datapages-module__list___2yM9o']//li/a")
                
                if i >= len(cancer_types):
                    print(f"[WARN] Index {i} out of range, skipping")
                    continue
                
                type_link = cancer_types[i]
                type_name = type_link.text.strip()
                print(f"[INFO] Processing cohort: {type_name}")
                
                # Extract cohort code
                cohort_code = extract_cohort_code_from_text(type_name)
                if not cohort_code:
                    print(f"[WARN] Could not extract cohort code from: {type_name}")
                    continue
                
                # Skip if not in target cohorts
                if cohort_code not in cohorts:
                    print(f"[SKIP] {cohort_code} not in target cohorts")
                    continue
                
                # Click on cancer type
                driver.execute_script("arguments[0].click();", type_link)
                time.sleep(2)
                
                # Look for gene expression RNAseq section
                try:
                    gene_expression_div = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, "//div[h3[contains(text(), 'gene expression RNAseq')]]"))
                    )
                    
                    # Find the ul element within the gene expression div
                    ul_element = WebDriverWait(gene_expression_div, timeout).until(
                        EC.presence_of_element_located((By.XPATH, ".//ul"))
                    )
                    
                    # Look for pancan normalized link
                    pancan_normalized_link = ul_element.find_element(By.XPATH, ".//li/a[contains(text(), 'pancan normalized')]")
                    driver.execute_script("arguments[0].click();", pancan_normalized_link)
                    time.sleep(2)
                    
                    # Find download link
                    download_link = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'PANCAN.gz')]"))
                    )
                    download_url = download_link.get_attribute("href")
                    
                    if download_url:
                        cohort_urls[cohort_code] = download_url
                        print(f"[OK] Found URL for {cohort_code}: {download_url}")
                    else:
                        print(f"[WARN] No download URL found for {cohort_code}")
                    
                except (TimeoutException, NoSuchElementException) as e:
                    print(f"[WARN] Could not find gene expression data for {cohort_code}: {e}")
                
                # Navigate back to main page
                driver.back()
                time.sleep(1)
                driver.back()
                time.sleep(2)
                
            except Exception as e:
                print(f"[ERROR] Error processing cohort {i + 1}: {e}")
                continue
        
        print(f"[INFO] Scraping completed. Found URLs for {len(cohort_urls)} cohorts")
        return cohort_urls
        
    except Exception as e:
        print(f"[ERROR] Selenium scraping failed: {e}")
        return {}
        
    finally:
        if driver:
            driver.quit()

def scrape_cohort_urls(cohorts: List[str] = None) -> Dict[str, str]:
    """
    Main function to get download URLs for TCGA cohorts.
    Uses Selenium-based scraping as primary method with fallback.
    """
    if cohorts is None:
        cohorts = CFG['tcga']['cohorts']
    
    print(f"Getting download URLs for cohorts: {cohorts}")
    
    # Try Selenium scraping first
    try:
        cohort_urls = scrape_cohort_urls_selenium(cohorts)
        if cohort_urls:
            return cohort_urls
    except Exception as e:
        print(f"[WARN] Selenium scraping failed: {e}")
    
    # Fallback: use hardcoded URLs for common cohorts
    print("[INFO] Using fallback URLs for common TCGA cohorts")
    fallback_urls = get_fallback_urls()
    
    cohort_urls = {}
    for cohort in cohorts:
        if cohort in fallback_urls:
            cohort_urls[cohort] = fallback_urls[cohort]
            print(f"[FALLBACK] Using fallback URL for {cohort}")
    
    if not cohort_urls:
        print("[WARN] No URLs available - neither Selenium nor fallback worked")
    
    return cohort_urls

def get_fallback_urls() -> Dict[str, str]:
    """Fallback URLs for common TCGA cohorts when Selenium fails."""
    return {
        'BRCA': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.BRCA.sampleMap%2FHiSeqV2_PANCAN.gz',
        'LUAD': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LUAD.sampleMap%2FHiSeqV2_PANCAN.gz',
        'COAD': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.COAD.sampleMap%2FHiSeqV2_PANCAN.gz',
        'GBM': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.GBM.sampleMap%2FHiSeqV2_PANCAN.gz',
        'LAML': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LAML.sampleMap%2FHiSeqV2_PANCAN.gz',
        'ACC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.ACC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'CHOL': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.CHOL.sampleMap%2FHiSeqV2_PANCAN.gz',
        'BLCA': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.BLCA.sampleMap%2FHiSeqV2_PANCAN.gz',
        'CESC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.CESC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'UCEC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.UCEC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'ESCA': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.ESCA.sampleMap%2FHiSeqV2_PANCAN.gz',
        'HNSC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.HNSC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'KICH': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.KICH.sampleMap%2FHiSeqV2_PANCAN.gz',
        'KIRC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.KIRC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'KIRP': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.KIRP.sampleMap%2FHiSeqV2_PANCAN.gz',
        'DLBC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.DLBC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'LIHC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LIHC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'LGG': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LGG.sampleMap%2FHiSeqV2_PANCAN.gz',
        'LUNG': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LUNG.sampleMap%2FHiSeqV2_PANCAN.gz',
        'LUSC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.LUSC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'SKCM': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.SKCM.sampleMap%2FHiSeqV2_PANCAN.gz',
        'MESO': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.MESO.sampleMap%2FHiSeqV2_PANCAN.gz',
        'UVM': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.UVM.sampleMap%2FHiSeqV2_PANCAN.gz',
        'OV': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.OV.sampleMap%2FHiSeqV2_PANCAN.gz',
        'PAAD': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.PAAD.sampleMap%2FHiSeqV2_PANCAN.gz',
        'PCPG': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.PCPG.sampleMap%2FHiSeqV2_PANCAN.gz',
        'PRAD': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.PRAD.sampleMap%2FHiSeqV2_PANCAN.gz',
        'READ': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.READ.sampleMap%2FHiSeqV2_PANCAN.gz',
        'SARC': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.SARC.sampleMap%2FHiSeqV2_PANCAN.gz',
        'STAD': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.STAD.sampleMap%2FHiSeqV2_PANCAN.gz',
        'TGCT': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.TGCT.sampleMap%2FHiSeqV2_PANCAN.gz',
        'THYM': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.THYM.sampleMap%2FHiSeqV2_PANCAN.gz',
        'THCA': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.THCA.sampleMap%2FHiSeqV2_PANCAN.gz',
        'UCS': 'https://tcga-xena-hub.s3.us-east-1.amazonaws.com/download/TCGA.UCS.sampleMap%2FHiSeqV2_PANCAN.gz'
    }

def get_cohort_download_url(cohort_code: str) -> Optional[str]:
    """Get download URL for specific cohort code using scraping."""
    urls = scrape_cohort_urls([cohort_code])
    return urls.get(cohort_code)

def get_available_cohorts() -> List[str]:
    """Get list of available TCGA cohorts by scraping."""
    print("Discovering available TCGA cohorts...")
    all_urls = scrape_cohort_urls(CFG['tcga']['cohorts'])
    return list(all_urls.keys())

def verify_url_availability(url: str, timeout: int = 10) -> bool:
    """Verify that a URL is accessible and returns a valid file."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        
        if response.status_code == 200:
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

def main():
    """Main function to test Selenium-based URL discovery."""
    print("TCGA Xena URL Discovery (Selenium)")
    print("=" * 40)
    
    # Get configured cohorts
    cohorts = CFG['tcga']['cohorts']
    print(f"Target cohorts: {cohorts}")
    
    # Get URLs for configured cohorts using Selenium scraping
    urls = scrape_cohort_urls(cohorts)
    
    print(f"\nFound URLs:")
    print("-" * 40)
    if urls:
        for cohort, url in urls.items():
            print(f"{cohort}: {url}")
        
        # Verify URLs
        print(f"\nVerifying URLs...")
        print("-" * 40)
        for cohort, url in urls.items():
            verify_url_availability(url)
    else:
        print("No URLs found. Make sure ChromeDriver is installed.")
        print("Install: pip install selenium")
        print("Download ChromeDriver from: https://chromedriver.chromium.org/")
    
    print(f"\nURL discovery complete. Found {len(urls)}/{len(cohorts)} cohorts.")

if __name__ == "__main__":
    main()