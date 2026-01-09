from app.services.json_reader import JsonIngestionService
from app.services.excel_reader import ExcelIngestionService

class IngestionController:
    def __init__(self):
        self.json_streamer = JsonIngestionService()
        self.excel_streamer = ExcelIngestionService()
