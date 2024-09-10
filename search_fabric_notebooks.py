import os
import concurrent.futures
import nbformat
from pathlib import Path

keyword = "sempy"

def search_notebook(notebook_path):
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)
        
        for cell in nb.cells:
            if cell.cell_type == 'code' and keyword in cell.source:
                return notebook_path
    except Exception as e:
        print(f"Error processing {notebook_path}: {str(e)}")
    return None

def scan_directory(directory):
    notebook_paths = list(Path(directory).rglob("*.ipynb"))
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(search_notebook, notebook_paths))
    
    return [result for result in results if result is not None]

root_directory = "/lakehouse/default/Files/notebooks2"  
results = scan_directory(root_directory)

if results:
    print(f"Notebooks containing {keyword}:")
    for notebook in results:
        print(notebook)
else:
    print(f"No notebooks found containing {keyword}.")    

