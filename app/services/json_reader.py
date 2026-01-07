import pandas as pd
import fsspec
from typing import List, Dict, Tuple

class JsonIngestionService:
    # This method is capable of reading multiple files from the folder [with read pagination]
    def read_paginated(self,path:str, page:int, page_size:int) -> Tuple[List[Dict],int, List[pd.DataFrame]]:
        """
        Stream JSON files from a file or dictonary and return : 
        - Paginated records 
        - Total row counts
        """
        fs, _, paths = fsspec.get_fs_token_paths(path)

        offset = (page - 1) * page_size
        limit = page_size

        collected : List[Dict] = []
        collected_dfs: List[pd.DataFrame] = []
        current_index = 0
        total_rows = 0

        for base_path in paths:
            files = (
                fs.glob(f"{base_path.rstrip('/')}/**/*.json")
                if fs.isdir(base_path)
                else [base_path]
            )

            for file in files:
                with fs.open(file,'r') as f:
                    df = pd.read_json(f,orient="records",dtype=False)

                records = df.to_dict(orient="records")

                for idx, record in enumerate(records):
                    # always count total rows
                    total_rows += 1
                    
                    # Skip until offset
                    if current_index < offset:
                        current_index += 1
                        continue 

                    # Collect page data 
                    if len(collected) < limit:
                        collected.append(record)
                        collected_dfs.append(df.iloc[[idx]])
                        current_index += 1
                    else:
                        # page is full --> stop early
                        return collected, total_rows, collected_dfs                   
        return collected, total_rows, collected_dfs
