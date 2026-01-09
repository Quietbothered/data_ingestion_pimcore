# # import pandas as pd
# # from typing import List, Dict, Tuple


# # class ExcelIngestionService:

# #     REQUIRED_COLUMNS = {
# #         "department": str,
# #         "department_desc": str,
# #         "zone": str,
# #         "zone_desc": str,
# #         "category": str,
# #         "category_desc": str,
# #         "coordinateGroup": str,
# #         "coordinateGroup_desc": str,
# #     }

# #     def read_paginated(
# #         self,
# #         file_path: str,
# #         page: int,
# #         page_size: int,
# #     ) -> Tuple[List[Dict], int]:

# #         offset = (page - 1) * page_size
# #         limit = page_size

# #         collected: List[Dict] = []
# #         total_rows = 0
# #         current_index = 0

# #         chunk_iter = pd.read_excel(
# #             file_path,
# #             engine="openpyxl",
# #             chunksize=2000
# #         )

# #         for chunk in chunk_iter:

# #             # schema validation (once)
# #             missing = set(self.REQUIRED_COLUMNS) - set(chunk.columns)
# #             if missing:
# #                 raise ValueError(f"Missing required columns: {missing}")

# #             # select + cast
# #             chunk = chunk[list(self.REQUIRED_COLUMNS.keys())]
# #             for col, dtype in self.REQUIRED_COLUMNS.items():
# #                 chunk[col] = chunk[col].astype(dtype, errors="ignore")

# #             records = chunk.to_dict(orient="records")

# #             for record in records:
# #                 total_rows += 1

# #                 if current_index < offset:
# #                     current_index += 1
# #                     continue

# #                 if len(collected) < limit:
# #                     collected.append(record)
# #                     current_index += 1
# #                 else:
# #                     return collected, total_rows

# #         return collected, total_rows
# import pandas as pd
# from typing import List, Dict, Tuple

# class ExcelIngestionService:

#     def read_paginated(
#         self,
#         path: str,
#         page: int,
#         page_size: int
#     ) -> Tuple[List[Dict], int, List[pd.DataFrame]]:

#         offset = (page - 1) * page_size
#         limit = page_size

#         collected: List[Dict] = []
#         page_dfs: List[pd.DataFrame] = []

#         total_rows = 0
#         current_index = 0

#         chunk_iter = pd.read_excel(
#             path,
#             engine="openpyxl",
#             chunksize=2000
#         )

#         for chunk in chunk_iter:
#             chunk.columns = [c.strip() for c in chunk.columns]
#             records = chunk.to_dict(orient="records")
#             if not records:
#                 continue
            
#             page_chunk_records = []
            
#             for record in records:
#                 total_rows += 1

#                 if current_index < offset:
#                     current_index += 1
#                     continue

#                 if len(collected) < limit:
#                     collected.append(record)
#                     page_chunk_records.append(record)
#                     current_index += 1
#                 else:
#                     if page_chunk_records:
#                         page_dfs.append(pd.DataFrame(page_chunk_records))
#                     return collected, total_rows, page_dfs
            
#             if page_chunk_records:
#                             page_dfs.append(pd.DataFrame(page_chunk_records))

#         if not page_dfs:
#             page_dfs = [pd.DataFrame(collected)]

#         return collected, total_rows, page_dfs
import pandas as pd
from typing import List, Dict, Tuple


class ExcelIngestionService:

    def read_paginated(
        self,
        path: str,
        page: int,
        page_size: int
    ) -> Tuple[List[Dict], int, List[pd.DataFrame]]:

        # Read entire Excel file ONCE
        df = pd.read_excel(path, engine="openpyxl")
        df.columns = [c.strip() for c in df.columns]

        total_rows = len(df)

        # Pagination math
        start = (page - 1) * page_size
        end = start + page_size

        page_df = df.iloc[start:end]

        records = page_df.to_dict(orient="records")

        # For memory monitoring compatibility
        page_dfs = [page_df]

        return records, total_rows, page_dfs
