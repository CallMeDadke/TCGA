"""
Generate visualizations from TCGA gene expression data in MongoDB.
Basic plots for cGAS-STING pathway analysis.
"""

import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for Docker
import pandas as pd
import numpy as np
import os
from typing import Dict, List, Optional, Tuple
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import CFG

# Target genes for cGAS-STING pathway
CGAS_STING_GENES = [
    'C6orf150', 'CCL5', 'CXCL10', 'TMEM173', 'CXCL9', 'CXCL11', 
    'NFKB1', 'IKBKE', 'IRF3', 'TREX1', 'ATM', 'IL6', 'IL8'
]

def get_mongo_client() -> MongoClient:
    """Initialize and return MongoDB client."""
    return MongoClient(CFG['mongo']['uri'])

def get_collection_stats() -> Dict:
    """Get basic statistics about the MongoDB collection."""
    print("Gathering collection statistics...")
    
    mongo_client = get_mongo_client()
    db = mongo_client[CFG['mongo']['db']]
    collection = db[CFG['mongo']['coll']]
    
    try:
        total_patients = collection.count_documents({})
        cohorts = collection.distinct("cancer_cohort")
        
        # Count patients per cohort
        cohort_counts = {}
        for cohort in cohorts:
            count = collection.count_documents({"cancer_cohort": cohort})
            cohort_counts[cohort] = count
        
        # Check for clinical data
        with_clinical = collection.count_documents({"clinical": {"$exists": True}})
        
        stats = {
            "total_patients": total_patients,
            "cohorts": sorted(cohorts),
            "cohort_counts": cohort_counts,
            "patients_with_clinical": with_clinical
        }
        
        return stats
        
    except PyMongoError as e:
        print(f"MongoDB error: {e}")
        return {}
    finally:
        mongo_client.close()

def get_gene_expression_data(gene: str, cohort: Optional[str] = None) -> List[float]:
    """Get expression values for a specific gene."""
    mongo_client = get_mongo_client()
    db = mongo_client[CFG['mongo']['db']]
    collection = db[CFG['mongo']['coll']]
    
    try:
        query = {f"genes.{gene}": {"$exists": True}}
        if cohort:
            query["cancer_cohort"] = cohort
        
        cursor = collection.find(query, {f"genes.{gene}": 1})
        values = []
        
        for doc in cursor:
            if 'genes' in doc and gene in doc['genes']:
                value = doc['genes'][gene]
                if isinstance(value, (int, float)) and not np.isnan(value):
                    values.append(float(value))
        
        return values
        
    except PyMongoError as e:
        print(f"MongoDB error getting gene data: {e}")
        return []
    finally:
        mongo_client.close()

def create_gene_expression_histogram(gene: str, cohort: Optional[str] = None) -> bool:
    """Create histogram of gene expression values."""
    print(f"Creating histogram for {gene}" + (f" in {cohort}" if cohort else ""))
    
    values = get_gene_expression_data(gene, cohort)
    
    if not values:
        print(f"No data found for gene {gene}")
        return False
    
    plt.figure(figsize=(10, 6))
    plt.hist(values, bins=50, alpha=0.7, edgecolor='black')
    
    title = f'{gene} Expression Distribution'
    if cohort:
        title += f' ({cohort})'
    
    plt.title(title)
    plt.xlabel('Expression Level (log2)')
    plt.ylabel('Number of Patients')
    plt.grid(True, alpha=0.3)
    
    # Add statistics
    mean_val = np.mean(values)
    median_val = np.median(values)
    plt.axvline(mean_val, color='red', linestyle='--', alpha=0.8, label=f'Mean: {mean_val:.2f}')
    plt.axvline(median_val, color='orange', linestyle='--', alpha=0.8, label=f'Median: {median_val:.2f}')
    plt.legend()
    
    # Save plot
    filename = f"{gene}_expression" + (f"_{cohort}" if cohort else "") + ".png"
    filepath = os.path.join("data", "plots", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Saved histogram: {filepath}")
    print(f"  Patients: {len(values)}, Mean: {mean_val:.2f}, Median: {median_val:.2f}")
    
    return True

def create_cohort_comparison_plot(gene: str) -> bool:
    """Create box plot comparing gene expression across cohorts."""
    print(f"Creating cohort comparison for {gene}")
    
    stats = get_collection_stats()
    if not stats.get("cohorts"):
        print("No cohort data available")
        return False
    
    cohort_data = {}
    for cohort in stats["cohorts"]:
        values = get_gene_expression_data(gene, cohort)
        if values:
            cohort_data[cohort] = values
    
    if not cohort_data:
        print(f"No expression data found for {gene}")
        return False
    
    plt.figure(figsize=(12, 8))
    
    # Prepare data for box plot
    box_data = []
    labels = []
    
    for cohort in sorted(cohort_data.keys()):
        box_data.append(cohort_data[cohort])
        labels.append(f"{cohort}\n(n={len(cohort_data[cohort])})")
    
    plt.boxplot(box_data, labels=labels)
    plt.title(f'{gene} Expression Across Cancer Types')
    plt.ylabel('Expression Level (log2)')
    plt.xlabel('Cancer Cohort')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    
    # Save plot
    filename = f"{gene}_cohort_comparison.png"
    filepath = os.path.join("data", "plots", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Saved cohort comparison: {filepath}")
    return True

def create_pathway_heatmap() -> bool:
    """Create heatmap of cGAS-STING pathway gene expression."""
    print("Creating cGAS-STING pathway heatmap")
    
    stats = get_collection_stats()
    if not stats.get("cohorts"):
        print("No cohort data available")
        return False
    
    # Collect expression data for all genes and cohorts
    expression_matrix = {}
    
    for cohort in stats["cohorts"]:
        expression_matrix[cohort] = {}
        
        for gene in CGAS_STING_GENES:
            values = get_gene_expression_data(gene, cohort)
            if values:
                expression_matrix[cohort][gene] = np.mean(values)
            else:
                expression_matrix[cohort][gene] = np.nan
    
    # Convert to DataFrame
    df = pd.DataFrame(expression_matrix).T  # Transpose so cohorts are rows
    
    if df.empty or df.isna().all().all():
        print("No pathway expression data available")
        return False
    
    plt.figure(figsize=(14, 8))
    
    # Create heatmap
    im = plt.imshow(df.values, aspect='auto', cmap='viridis', interpolation='nearest')
    
    # Set labels
    plt.xticks(range(len(df.columns)), df.columns, rotation=45, ha='right')
    plt.yticks(range(len(df.index)), df.index)
    
    plt.title('cGAS-STING Pathway Gene Expression\n(Mean Expression by Cohort)')
    plt.xlabel('Genes')
    plt.ylabel('Cancer Cohorts')
    
    # Add colorbar
    cbar = plt.colorbar(im)
    cbar.set_label('Mean Expression Level (log2)')
    
    # Add text annotations
    for i in range(len(df.index)):
        for j in range(len(df.columns)):
            value = df.iloc[i, j]
            if not np.isnan(value):
                plt.text(j, i, f'{value:.1f}', ha='center', va='center', 
                        color='white' if value < df.values[~np.isnan(df.values)].mean() else 'black')
    
    # Save plot
    filepath = os.path.join("data", "plots", "cgas_sting_pathway_heatmap.png")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    plt.tight_layout()
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Saved pathway heatmap: {filepath}")
    return True

def print_summary_stats():
    """Print summary statistics about the dataset."""
    print("\nDataset Summary")
    print("=" * 40)
    
    stats = get_collection_stats()
    
    if not stats:
        print("No data available in MongoDB")
        return
    
    print(f"Total Patients: {stats['total_patients']}")
    print(f"Cancer Cohorts: {len(stats['cohorts'])}")
    print(f"Patients with Clinical Data: {stats['patients_with_clinical']}")
    
    print("\nPatients per Cohort:")
    for cohort, count in sorted(stats['cohort_counts'].items()):
        print(f"  {cohort}: {count}")
    
    # Gene availability
    print(f"\nGene Expression Coverage:")
    mongo_client = get_mongo_client()
    db = mongo_client[CFG['mongo']['db']]
    collection = db[CFG['mongo']['coll']]
    
    try:
        for gene in CGAS_STING_GENES:
            count = collection.count_documents({f"genes.{gene}": {"$exists": True}})
            percentage = (count / stats['total_patients']) * 100 if stats['total_patients'] > 0 else 0
            print(f"  {gene}: {count} patients ({percentage:.1f}%)")
    except Exception as e:
        print(f"Error getting gene coverage: {e}")
    finally:
        mongo_client.close()

def create_demo_plots():
    """Create demo plots with synthetic data when MongoDB is unavailable."""
    print("Creating demo visualizations with synthetic data...")
    
    import numpy as np
    np.random.seed(42)  # For reproducible demo data
    
    # Create plots directory
    os.makedirs("data/plots", exist_ok=True)
    plots_created = 0
    
    # Demo histogram
    plt.figure(figsize=(10, 6))
    
    # Generate synthetic gene expression data
    normal_expr = np.random.normal(3.0, 1.5, 500)  # Normal tissue
    tumor_expr = np.random.normal(4.5, 1.8, 800)   # Tumor tissue
    
    plt.hist(normal_expr, bins=30, alpha=0.7, label='Normal', color='blue')
    plt.hist(tumor_expr, bins=30, alpha=0.7, label='Tumor', color='red')
    
    plt.title('TMEM173 Gene Expression Distribution (Demo Data)')
    plt.xlabel('Expression Level (log2)')
    plt.ylabel('Number of Samples')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    filepath = os.path.join("data", "plots", "TMEM173_expression_demo.png")
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    plots_created += 1
    print(f"Created demo histogram: {filepath}")
    
    # Demo heatmap
    plt.figure(figsize=(12, 8))
    
    # Create synthetic pathway expression data
    genes = ['TMEM173', 'CCL5', 'CXCL10', 'IL6', 'IL8', 'NFKB1', 'IRF3']
    cohorts = ['BRCA', 'LUAD', 'COAD', 'GBM', 'LAML']
    
    # Generate expression matrix
    expression_data = np.random.normal(3.0, 2.0, (len(cohorts), len(genes)))
    expression_data = np.maximum(expression_data, 0)  # Ensure non-negative
    
    im = plt.imshow(expression_data, aspect='auto', cmap='viridis')
    
    plt.xticks(range(len(genes)), genes, rotation=45, ha='right')
    plt.yticks(range(len(cohorts)), cohorts)
    
    plt.title('cGAS-STING Pathway Gene Expression (Demo Data)')
    plt.xlabel('Genes')
    plt.ylabel('Cancer Cohorts')
    
    # Add colorbar
    cbar = plt.colorbar(im)
    cbar.set_label('Expression Level (log2)')
    
    # Add value annotations
    for i in range(len(cohorts)):
        for j in range(len(genes)):
            plt.text(j, i, f'{expression_data[i, j]:.1f}', 
                    ha='center', va='center', color='white')
    
    filepath = os.path.join("data", "plots", "cgas_sting_pathway_demo.png")
    plt.tight_layout()
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    plots_created += 1
    print(f"Created demo heatmap: {filepath}")
    
    return plots_created

def main():
    """Main visualization function."""
    print("TCGA Gene Expression Visualization")
    print("=" * 40)
    
    try:
        # Print summary statistics
        print_summary_stats()
        
        # Get basic stats
        stats = get_collection_stats()
        
        if stats['total_patients'] == 0:
            print("\nNo data available for visualization!")
            print("Please run the data pipeline first:")
            print("  python src/download_to_minio.py")
            print("  python src/transform_to_mongo.py")
            return
            
    except Exception as e:
        print(f"\nMongoDB connection error: {e}")
        print("\nRunning demo mode with synthetic data...")
        plots_created = create_demo_plots()
        print(f"\nDemo visualization completed!")
        print(f"Created {plots_created} demo plots in data/plots/")
        return
    
    print(f"\nGenerating visualizations...")
    
    # Create plots directory
    os.makedirs("data/plots", exist_ok=True)
    
    plots_created = 0
    
    # Create histograms for key genes
    key_genes = ['TMEM173', 'CCL5', 'CXCL10', 'IL6', 'IL8']  # Most important cGAS-STING genes
    
    for gene in key_genes:
        if create_gene_expression_histogram(gene):
            plots_created += 1
    
    # Create cohort comparison for TMEM173 (cGAS)
    if create_cohort_comparison_plot('TMEM173'):
        plots_created += 1
    
    # Create pathway heatmap
    if create_pathway_heatmap():
        plots_created += 1
    
    print(f"\nVisualization completed!")
    print(f"Created {plots_created} plots in data/plots/")
    
    if plots_created > 0:
        print("\nGenerated plots:")
        plot_dir = os.path.join("data", "plots")
        if os.path.exists(plot_dir):
            for filename in os.listdir(plot_dir):
                if filename.endswith('.png'):
                    print(f"  {filename}")

if __name__ == "__main__":
    main()