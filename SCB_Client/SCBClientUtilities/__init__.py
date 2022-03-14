from typing import List

def flatten_data(nested_data: List[list]) -> list:
  return [item for sublist in nested_data for item in sublist]