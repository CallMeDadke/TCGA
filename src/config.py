"""
Configuration module for TCGA bioinformatics pipeline.
Loads environment variables and provides configuration dictionary.
"""

import os
from dotenv import load_dotenv

load_dotenv()

CFG = {
    'minio': {
        'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
        'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'bucket': os.getenv('MINIO_BUCKET', 'tcga'),
        'secure': os.getenv('MINIO_SECURE', 'False').lower() == 'true'
    },
    'mongo': {
        'uri': os.getenv('MONGODB_URI', 'mongodb+srv://<user>:<password>@cluster0.vwod2pa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'),
        'db': os.getenv('MONGODB_DB', 'tcga'),
        'coll': os.getenv('MONGODB_COLL', 'gene_expression')
    },
    'tcga': {
        'cohorts': ['BRCA', 'LUAD', 'COAD', 'GBM', 'LAML', 'ACC',
                    'CHOL', 'BLCA', 'CESC', 'UCEC', 'ESCA', 'HNSC',
                    'KICH', 'KIRC', 'KIRP', 'DLBC', 'LIHC', 'LGG',
                    'LUNG', 'LUSC', 'SKCM', 'MESO', 'UVM', 'OV', 'PAAD',
                    'PCPG', 'PRAD', 'READ', 'SARC', 'STAD', 'TGCT', 'THYM', 'THCA', 'UCS'
                    ],
        'data_type': os.getenv('DATA_TYPE', 'IlluminaHiSeq_RNASeqV2'),
        'download_dir': os.getenv('DOWNLOAD_DIR', 'data/tmp')
    },
    'cgas_sting_genes': [
        'C6orf150', 'CCL5', 'CXCL10', 'TMEM173', 'CXCL9', 'CXCL11',
        'NFKB1', 'IKBKE', 'IRF3', 'TREX1', 'ATM', 'IL6', 'IL8', 'CXCL8'
    ]
}


def get_config():
    """Return configuration dictionary."""
    return CFG
