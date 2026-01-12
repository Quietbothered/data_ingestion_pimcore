from enum import Enum
class ErrorMessages(Enum):
    # default server errors
    INTERNAL_SERVER_ERROR = "Internal server error"
    
    # custom server errors
    FILE_URL_IS_NONE = "File url is required!"
    FILE_TYPE_IS_NONE = "File type is required!"
    NEITHER_CHUNK_SIZE_PROVIDED = "Either chunk_size_by_records or chunk_size_by_memory must be provided"
    BOTH_CHUNK_SIZES_PROVIDED = "Provide only one: chunk_size_by_records OR chunk_size_by_memory"
    CALL_BACK_URL_IS_NONE = "Callback url is required!"

    # error message sent by pim-core in the response
    OUT_OF_ORDER_CHUNK = "Out-of-order chunk"