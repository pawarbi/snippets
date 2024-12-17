from deltalake import DeltaTable
from datetime import datetime, timedelta
import notebookutils

def maintain_delta_table(table_path: str) -> dict:
    """
    Maintain a Delta table by checking its history and performing optimize/vacuum operations when needed.
    
    Args:
        table_path (str): Path to the Delta table (abfss:// format)
        
    Returns:
        dict: Summary of operations performed
    """
    # Setup storage options for Azure
    storage_options = {
        "bearer_token": notebookutils.credentials.getToken("storage"),
        "use_fabric_endpoint": "true"
    }
    
    # Initialize Delta table
    dt = DeltaTable(table_path, storage_options=storage_options)
    
    # Get table history
    history = dt.history()
    current_time = datetime.now()
    
    # Initialize results dictionary
    results = {
        "optimize_performed": False,
        "vacuum_performed": False,
        "optimize_last_run": None,
        "vacuum_last_run": None,
        "message": []
    }
    
    # Find last optimize and vacuum operations
    last_optimize = None
    last_vacuum = None
    
    for entry in history:
        # History entries are already dictionaries
        if "OPTIMIZE" in entry['operation']:
            if not last_optimize or entry['timestamp'] > last_optimize:
                last_optimize = datetime.fromtimestamp(entry['timestamp'] / 1000)  # Convert milliseconds to datetime
        elif "VACUUM" in entry['operation']:
            if not last_vacuum or entry['timestamp'] > last_vacuum:
                last_vacuum = datetime.fromtimestamp(entry['timestamp'] / 1000)  # Convert milliseconds to datetime
    
    # Store last run times in results
    results["optimize_last_run"] = last_optimize.isoformat() if last_optimize else None
    results["vacuum_last_run"] = last_vacuum.isoformat() if last_vacuum else None
    
    # Check if optimize is needed (if last run was more than 7 days ago or never run)
    optimize_needed = not last_optimize or \
                     (current_time - last_optimize) > timedelta(days=7)
    
    if optimize_needed:
        try:
            dt.optimize()
            results["optimize_performed"] = True
            results["message"].append("Optimize operation completed successfully")
        except Exception as e:
            results["message"].append(f"Optimize operation failed: {str(e)}")
    else:
        results["message"].append("Optimize operation skipped - last run was within 7 days")
    
    # Check if vacuum is needed (if last run was more than 30 days ago or never run)
    vacuum_needed = not last_vacuum or \
                   (current_time - last_vacuum) > timedelta(days=30)
    
    if vacuum_needed:
        try:
            dt.vacuum()  # You might want to add retention_hours parameter if needed
            results["vacuum_performed"] = True
            results["message"].append("Vacuum operation completed successfully")
        except Exception as e:
            results["message"].append(f"Vacuum operation failed: {str(e)}")
    else:
        results["message"].append("Vacuum operation skipped - last run was within 30 days")
    
    return results

# Example usage:
if __name__ == "__main__":
    table_path = "abfss://container@storage.dfs.fabric.microsoft.com/path/to/table"
    result = maintain_delta_table(table_path)
    print("Maintenance Summary:")
    print(f"Optimize Last Run: {result['optimize_last_run']}")
    print(f"Vacuum Last Run: {result['vacuum_last_run']}")
    print(f"Optimize Performed: {result['optimize_performed']}")
    print(f"Vacuum Performed: {result['vacuum_performed']}")
    print("\nOperation Messages:")
    for msg in result['message']:
        print(f"- {msg}")
