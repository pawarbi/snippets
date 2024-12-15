import concurrent.futures
from typing import List, Dict, Any
import logging
import multiprocessing
import time

def refresh_semantic_models(dataset_config: List[Dict[Any, Any]], 
                          mode: str = "sequential", 
                          batch_size: int = None,
                          wait: int = 0) -> List[Dict[str, Any]]:
    """
    Refresh semantic models based on configuration either sequentially or concurrently.
    
    Args:
        dataset_config: List of dataset configuration dictionaries
        mode: "sequential" or "concurrent"
        batch_size: Number of concurrent refreshes to trigger in each batch (defaults to CPU count)
        wait: Time in seconds to wait between refreshes or batches
    
    Returns:
        List of refresh results containing dataset name and status
    """
    results = []
    
    def refresh_single_dataset(config: Dict[Any, Any], max_retries: int = 1) -> Dict[str, Any]:
        """Helper function to refresh a single dataset and handle errors with retry logic"""
        dataset_name = config["dataset"]
        for attempt in range(max_retries + 1):
            try:
                logging.info(f"Starting refresh for dataset: {dataset_name} (Attempt {attempt + 1}/{max_retries + 1})")
                fabric.refresh_dataset(**config)
                return {
                    "dataset": dataset_name,
                    "status": "success"
                }
            except Exception as e:
                if "clr" in str(e) and attempt < max_retries:
                    logging.warning(f"CLR initialization error for {dataset_name}, retrying...")
                    continue
                return {
                    "dataset": dataset_name,
                    "status": "failed",
                    "error": str(e)
                }

    if mode == "sequential":
        # Process datasets one at a time in order
        for config in dataset_config:
            result = refresh_single_dataset(config)
            results.append(result)
            if wait > 0:
                logging.info(f"Waiting {wait} seconds before next refresh...")
                time.sleep(wait)
            
    elif mode == "concurrent":
        # If batch_size is not specified, use number of CPU cores
        if batch_size is None:
            batch_size = multiprocessing.cpu_count()
            logging.info(f"Using {batch_size} CPU cores for concurrent processing")
        
        # Warm-up: Process first dataset sequentially to ensure CLR initialization
        if dataset_config:
            first_result = refresh_single_dataset(dataset_config[0])
            results.append(first_result)
            logging.info(f"Completed warm-up refresh of {dataset_config[0]['dataset']}")
            
            # Process remaining datasets concurrently
            remaining_configs = dataset_config[1:]
            if remaining_configs:
                logging.info(f"Processing remaining {len(remaining_configs)} datasets concurrently")
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                    # Split remaining configs into batches
                    for i in range(0, len(remaining_configs), batch_size):
                        batch = remaining_configs[i:i + batch_size]
                        
                        # Submit batch of refresh tasks
                        future_to_config = {
                            executor.submit(refresh_single_dataset, config): config 
                            for config in batch
                        }
                        
                        # Wait for batch completion
                        for future in concurrent.futures.as_completed(future_to_config):
                            config = future_to_config[future]
                            try:
                                result = future.result()
                                results.append(result)
                            except Exception as e:
                                results.append({
                                    "dataset": config["dataset"],
                                    "status": "failed",
                                    "error": f"Execution error: {str(e)}"
                                })
                                
                        if wait > 0 and i + batch_size < len(remaining_configs):
                            logging.info(f"Waiting {wait} seconds before next batch...")
                            time.sleep(wait)
    else:
        raise ValueError("Mode must be either 'sequential' or 'concurrent'")
        
    return results

# Example usage:
"""
# Sequential refresh with 30 second wait between refreshes
results = refresh_semantic_models(dataset_config, mode="sequential", wait=30)

# Concurrent refresh with 60 second wait between batches
results = refresh_semantic_models(
    dataset_config, 
    mode="concurrent",
    batch_size=4,
    wait=60
)
"""
