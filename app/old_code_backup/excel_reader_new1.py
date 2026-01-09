from typing import List, Dict, Iterator
from openpyxl import load_workbook


class ExcelIngestionService:
    def read_in_chunks(
        self,
        file_path: str,
        chunk_size: int = 1000
    ) -> Iterator[List[Dict]]:
        """
        Stream Excel rows without assuming column names or order.
        Yields chunks of row dictionaries.
        """

        wb = load_workbook(
            filename=file_path,
            read_only=True,
            data_only=True
        )

        sheet = wb.active
        rows = sheet.iter_rows(values_only=True)

        # ---- Read header dynamically ----
        header_row = next(rows, None)
        if not header_row:
            wb.close()
            return

        headers = [
            str(col).strip() if col is not None else f"column_{i}"
            for i, col in enumerate(header_row)
        ]

        chunk: List[Dict] = []

        for row in rows:
            record = {
                headers[i]: row[i]
                for i in range(len(headers))
            }

            chunk.append(record)

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

        wb.close()
