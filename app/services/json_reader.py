import pandas as pd
import ijson
import fsspec
from typing import List, Dict, Tuple, Optional

class JsonIngestionService:
    # This method is capable of reading multiple files from the folder [with read pagination] [old logic : without the support for chunk_size_by_memory]
    # def read_paginated(self,path:str, page:int, chunk_size_by_records:int) -> Tuple[List[Dict],int, List[pd.DataFrame]]:
    #     """
    #     Stream JSON files from a file or dictonary and return : 
    #     - Paginated records 
    #     - Total row counts
    #     """
    #     fs, _, paths = fsspec.get_fs_token_paths(path)

    #     offset = (page - 1) * chunk_size_by_records
    #     limit = chunk_size_by_records

    #     # page records
    #     collected : List[Dict] = []
    #     # Ro-level dataframes for memory calculations
    #     collected_dfs: List[pd.DataFrame] = []
    #     # global row counter
    #     current_index = 0
    #     # count all rows access all files
    #     total_rows = 0

    #     # find all the files in a directory via looping
    #     for base_path in paths:
    #         files = (
    #             fs.glob(f"{base_path.rstrip('/')}/**/*.json")
    #             if fs.isdir(base_path)
    #             else [base_path]
    #         )

    #         for file in files:
    #             # read each file inside the current directory
    #             with fs.open(file,'r') as f:
    #                 df = pd.read_json(f,orient="records",dtype=False)

    #             # record level streaming loop
    #             records = df.to_dict(orient="records")

    #             for idx, record in enumerate(records):
    #                 # always count total rows
    #                 total_rows += 1
                    
    #                 # Skip until offset
    #                 if current_index < offset:
    #                     current_index += 1
    #                     continue 

    #                 # Collect page data 
    #                 if len(collected) < limit:
    #                     collected.append(record)
    #                     collected_dfs.append(df.iloc[[idx]])
    #                     current_index += 1
    #                 else:
    #                     # page is full --> stop early
    #                     return collected, total_rows, collected_dfs                   
    #     return collected, total_rows, collected_dfs

    # This method is capable of reading multiple files from the folder with read pagination [with chunk_size_by_memory support]
    """
    When chunk_size_by_memory is supplied this method will automatically determine the number of rows each chunk must have 
    """
    def read_paginated(
        self,
        path: str,
        page: int,
        chunk_size_by_records: Optional[int] = None,
        chunk_size_by_memory: Optional[int] = None
    ) -> Tuple[List[Dict], int, List[pd.DataFrame]]:

        fs, _, paths = fsspec.get_fs_token_paths(path)

        collected: List[Dict] = []
        collected_dfs: List[pd.DataFrame] = []

        current_index = 0
        total_rows = 0
        current_memory_bytes = 0

        for base_path in paths:
            files = (
                fs.glob(f"{base_path.rstrip('/')}/**/*.json")
                if fs.isdir(base_path)
                else [base_path]
            )

            for file in files:
                with fs.open(file, "rb") as f:
                    # STREAM objects inside JSON array
                    for record in ijson.items(f, "item"):
                        total_rows += 1

                        # Pagination (record-based)
                        if chunk_size_by_records and current_index < (page - 1) * chunk_size_by_records:
                            current_index += 1
                            continue

                        record_df = pd.DataFrame([record])
                        record_memory = record_df.memory_usage(deep=True).sum()

                        # Stop conditions
                        if chunk_size_by_records and len(collected) >= chunk_size_by_records:
                            return collected, total_rows, collected_dfs

                        if chunk_size_by_memory and current_memory_bytes + record_memory > chunk_size_by_memory:
                            return collected, total_rows, collected_dfs

                        collected.append(record)
                        collected_dfs.append(record_df)
                        current_memory_bytes += record_memory
                        current_index += 1

        return collected, total_rows, collected_dfs


