import concurrent.futures
from typing import List, Dict, Any
import logging

def refresh_semantic_models(dataset_config: List[Dict[Any, Any]], 
                          mode: str = "sequential", 
                          batch_size: int = 4,
                          timeout: int = 3600) -> List[Dict[str, Any]]:
    """
    Refresh semantic models based on configuration either sequentially or concurrently.
    
    Args:
        dataset_config: List of dataset configuration dictionaries
        mode: "sequential" or "concurrent"
        batch_size: Number of concurrent refreshes to trigger in each batch
        timeout: Maximum time in seconds to wait for each refresh batch
    
    Returns:
        List of refresh results containing dataset name and status
    """
    results = []
    
    def refresh_single_dataset(config: Dict[Any, Any]) -> Dict[str, Any]:
        """Helper function to refresh a single dataset and handle errors"""
        try:
            dataset_name = config["dataset"]
            logging.info(f"Starting refresh for dataset: {dataset_name}")
            fabric.refresh_dataset(**config)
            return {
                "dataset": dataset_name,
                "status": "success"
            }
        except Exception as e:
            logging.error(f"Error refreshing dataset {dataset_name}: {str(e)}")
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
            
    elif mode == "concurrent":
        # Process datasets in batches using concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Split configs into batches
            for i in range(0, len(dataset_config), batch_size):
                batch = dataset_config[i:i + batch_size]
                
                # Submit batch of refresh tasks
                future_to_config = {
                    executor.submit(refresh_single_dataset, config): config 
                    for config in batch
                }
                
                # Wait for batch completion with timeout
                try:
                    for future in concurrent.futures.as_completed(future_to_config, timeout=timeout):
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
                except concurrent.futures.TimeoutError:
                    for future in future_to_config:
                        future.cancel()
                    logging.error(f"Timeout occurred while processing batch starting at index {i}")
    else:
        raise ValueError("Mode must be either 'sequential' or 'concurrent'")
        
    return results

# Example usage:
"""
# Sequential refresh
results = refresh_semantic_models(dataset_config, mode="sequential")

# Concurrent refresh with custom batch size
results = refresh_semantic_models(
    dataset_config, 
    mode="concurrent",
    batch_size=4,
    timeout=7200  # 2 hours timeout
)

# Process results
for result in results:
    if result["status"] == "success":
        print(f"Successfully refreshed {result['dataset']}")
    else:
        print(f"Failed to refresh {result['dataset']}: {result.get('error', 'Unknown error')}")
"""
