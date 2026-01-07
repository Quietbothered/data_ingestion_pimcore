import pandas as pd


class DataFrameMemoryService:
    """
    Responsible only for calculating memory usage of pandas DataFrames
    """

    @staticmethod
    def calculate_bytes(df: pd.DataFrame) -> int:
        """
        Returns memory usage of a DataFrame in bytes
        """
        return int(df.memory_usage(deep=True).sum())
