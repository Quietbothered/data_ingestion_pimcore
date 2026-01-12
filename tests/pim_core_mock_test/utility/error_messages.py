from enum import Enum 
class ErrorMessages(Enum):
    # CHUNK DATA INTEGRITY ERRORS
    OUT_OF_ORDER_CHUNK = "Out-of-order chunk"
    CHECKSUM_MISMATCH = "Checksum mismatch"

    # CHUNK drop error 
    EMPTY_CHUNK = "Empty chunk"