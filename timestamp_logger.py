import os
import time
import json
from datetime import datetime

def timestamp_logger(folder, tag):
    """
    Logs the current timestamp to a file in S3
    
    Args:
        folder (str): Folder name in S3
        tag (str): Tag to identify the platform
    """
    
    t = datetime.now().isoformat()
    id = faasr_invocation_id()
    
    filename = f"{tag}_{id}.log"
    local_filepath = f"/tmp/{filename}"
    
    content = {
        "platform": tag,
        "invocation_id": id,
        "timestamp": t
    }
    
    with open(local_filepath, 'w') as f:
        f.write(json.dumps(content, indent=2))
    
    faasr_log(f"Created timestamp file: {filename} with timestamp: {t}")
    
    try:
        faasr_put_file(
            local_file=filename,
            remote_file=filename,
            local_folder="/tmp",
            remote_folder=folder
        )
        
        faasr_log(f"Successfully uploaded to S3: {folder}/{filename}")
        return True
    except Exception as e:
        faasr_log(f"Error uploading to S3: {str(e)}")
        return False
