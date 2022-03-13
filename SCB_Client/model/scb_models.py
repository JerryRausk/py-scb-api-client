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
  elimination: bool = False
  time: bool = False

@dataclass
class SCBResponseDataPoint:
  key: List[str]
  values: List[str]

@dataclass
class SCBResponse:
  columns: List[dict]
  comments: List[dict]
  data: List[SCBResponseDataPoint]

@dataclass
class SCBQueryVariableSelection:
  filter: str
  values: List[str]
@dataclass
class SCBQueryVariable:
  code: str
  selection: SCBQueryVariableSelection

@dataclass
class SCBQuery:
  query: List[SCBQueryVariable]
  response_type: ResponseType
  
  def query_variables_to_list(self):
    ret = []
    for var in self.query:
      ret.append(
        {
          "code": var.code,
          "selection": {
            "filter": var.selection.filter,
            "values": var.selection.values
          }
        }
      )
    return ret
  
  def query_variable_codes_to_list(self):
    return [var.code for var in self.query]

  def to_dict(self):
    return {
      "query" : self.query_variables_to_list(),
      "response": {
        "format": self.response_type.value
      } 
    }
