# json decimal encoder
"""
This function allows us to parse the decimal values in json files 
"""
import decimal
def orjson_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)   # OR: return str(obj)
    raise TypeError