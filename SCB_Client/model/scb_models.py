from enum import Enum
from dataclasses import dataclass
from typing import List

class ResponseType(Enum):
  JSON = "json"

@dataclass()
class SCBVariable():
  code: str
  text: str
  values: List[str]
  valueTexts: List[str]

@dataclass
class SCBResponseDataPoint:
  key: List[str]
  values: List[str]

@dataclass
class SCBResponse:
  columns: List[dict]
  comments: List[dict]
  data: List[SCBResponseDataPoint]

class SCBQuery:
  def __init__(self, query: dict[str, list], response_type: str):
    self.query = query
    self.response_type = response_type
