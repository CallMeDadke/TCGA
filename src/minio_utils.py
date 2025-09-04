"""
MinIO utilities for TCGA pipeline.
Provides S3-compatible object storage operations.
"""

from minio import Minio
from minio.error import S3Error
import io
from typing import List, Optional, Generator
from config import CFG

def get_minio_client() -> Minio:
    """Initialize and return MinIO client."""
    return Minio(
        endpoint=CFG['minio']['endpoint'],
        access_key=CFG['minio']['access_key'],
        secret_key=CFG['minio']['secret_key'],
        secure=CFG['minio']['secure']
    )

def ensure_bucket_exists(client: Optional[Minio] = None, bucket_name: Optional[str] = None) -> bool:
    """Ensure bucket exists, create if it doesn't."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        return True
    except S3Error as e:
        print(f"Error ensuring bucket exists: {e}")
        return False

def upload_file(file_path: str, object_name: str, bucket_name: Optional[str] = None, 
                client: Optional[Minio] = None) -> bool:
    """Upload a file to MinIO bucket."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        ensure_bucket_exists(client, bucket_name)
        client.fput_object(bucket_name, object_name, file_path)
        print(f"Uploaded {file_path} to {bucket_name}/{object_name}")
        return True
    except S3Error as e:
        print(f"Error uploading file: {e}")
        return False

def upload_data(data: bytes, object_name: str, bucket_name: Optional[str] = None,
                client: Optional[Minio] = None) -> bool:
    """Upload data bytes to MinIO bucket."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        ensure_bucket_exists(client, bucket_name)
        data_stream = io.BytesIO(data)
        client.put_object(bucket_name, object_name, data_stream, len(data))
        print(f"Uploaded {len(data)} bytes to {bucket_name}/{object_name}")
        return True
    except S3Error as e:
        print(f"Error uploading data: {e}")
        return False

def download_file(object_name: str, file_path: str, bucket_name: Optional[str] = None,
                  client: Optional[Minio] = None) -> bool:
    """Download a file from MinIO bucket."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        client.fget_object(bucket_name, object_name, file_path)
        print(f"Downloaded {bucket_name}/{object_name} to {file_path}")
        return True
    except S3Error as e:
        print(f"Error downloading file: {e}")
        return False

def get_object_data(object_name: str, bucket_name: Optional[str] = None,
                    client: Optional[Minio] = None) -> Optional[bytes]:
    """Get object data as bytes from MinIO bucket."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    except S3Error as e:
        print(f"Error getting object data: {e}")
        return None

def get_object_stream(object_name: str, bucket_name: Optional[str] = None,
                     client: Optional[Minio] = None):
    """Get object as stream from MinIO bucket."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        return client.get_object(bucket_name, object_name)
    except S3Error as e:
        print(f"Error getting object stream: {e}")
        return None

def list_objects(prefix: str = "", bucket_name: Optional[str] = None,
                client: Optional[Minio] = None) -> List[str]:
    """List objects in bucket with given prefix."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except S3Error as e:
        print(f"Error listing objects: {e}")
        return []

def list_prefix(prefix: str, bucket_name: Optional[str] = None,
               client: Optional[Minio] = None) -> Generator[str, None, None]:
    """Generator that yields object names with given prefix."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        for obj in objects:
            yield obj.object_name
    except S3Error as e:
        print(f"Error listing prefix: {e}")

def object_exists(object_name: str, bucket_name: Optional[str] = None,
                 client: Optional[Minio] = None) -> bool:
    """Check if object exists in bucket."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except S3Error:
        return False

def delete_object(object_name: str, bucket_name: Optional[str] = None,
                 client: Optional[Minio] = None) -> bool:
    """Delete object from bucket."""
    if client is None:
        client = get_minio_client()
    if bucket_name is None:
        bucket_name = CFG['minio']['bucket']
    
    try:
        client.remove_object(bucket_name, object_name)
        print(f"Deleted {bucket_name}/{object_name}")
        return True
    except S3Error as e:
        print(f"Error deleting object: {e}")
        return False