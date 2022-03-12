import json
from typing import Dict, List
import requests
import math
from time import sleep
from SCB_Client.model.scb_models import SCBQuery, SCBResponse, SCBResponseDataPoint, SCBVariable, ResponseType


SCB_LIMIT_RESULT = 150000
SCB_BASE_URL = "https://api.scb.se/OV0104/v1/doris/sv/ssd"

class SCBClient:
  base_url = SCB_BASE_URL
  _variables: List[SCBVariable] = None # Used to cache variableas in case they are needed multiple times
  _size_limit_cells = 30000
  _preferred_partition_variable: SCBVariable = None

  def __init__(
    self, 
    area: str, 
    category: str,
    category_specification: str,
    table: str
    ):
 
    self.area = area
    self.category = category
    self.category_specification = category_specification
    self.table = table
    self.data_url = f"{self.base_url}/{area}/{category}/{category_specification}/{table}"

  def set_preferred_partition_variable(self, variable_code: str) -> None:
    """preferred_partition_variable will be used to partition the requests if the expected result is larger than the SCB limit."""
    valid_variables = [var.code for var in self.get_variables()]
    if variable_code not in valid_variables:
      raise ValueError(f"{variable_code} is not a valid variable, the valid variables are {', '.join(valid_variables)}")
    self._preferred_partition_variable = [var for var in self.get_variables() if var.code == variable_code][0]

  def get_preferred_partition_variable(self) -> SCBVariable:
    """preferred_partition_variable will be used to partition the requests if the expected result is larger than the SCB limit."""
    return self._preferred_partition_variable

  def set_size_limit(self, limit: int) -> None:
    """Size limit is used to protect against unwanted data use, will be ignored if set to 0."""
    if limit < 0 or not isinstance(limit, int):
      raise ValueError("Size limit must be a positive integer.")
    self._size_limit_cells = limit 
  
  def get_size_limit(self) -> int:
    """Size limit is used to protect against unwanted data use, will be ignored if set to 0."""
    return self._size_limit_cells

  def estimate_cell_count(self, query: SCBQuery) -> int:
    """
    A lightweight get request will be made to SCB to calculate the number of
    "cells" that will be returned with this selection.
    Params:
      query:
        An instance of SCBQuery
    Returns:
      count: int
    """
    query_variables = query.query
    variables = self.get_variables()
    # This dict will be used to count the product of all the combinations. 
    variables_dict = {}
    for var in variables:
      # If the variable isnt included in the query it means it is omitted by the user when creating the query.
      if var.code in [query_var["code"] for query_var in query_variables]:
       variables_dict[var.code] = var.values
    
    for var in query_variables:
      if var["selection"]["values"] != ["*"]:
        variables_dict[var["code"]] = var["selection"]["values"]

    combinations = [len(v) for _, v in variables_dict.items()]
    return math.prod(combinations)
  
  def get_data(self, query: SCBQuery) -> List[SCBResponse]:
    """
    Get data from SCB if internal limit is not exceeded. 
    Multiple requests will be made if the SCB limit is exceeded.
    """
    estimated_cell_count = self.estimate_cell_count(query)
    if estimated_cell_count > self._size_limit_cells and self._size_limit_cells > 0:
      raise PermissionError(f"Current size limit {self._size_limit_cells} will be exceeded. The size limit can be changed with set_size_limit().")
    if estimated_cell_count < SCB_LIMIT_RESULT:
      response = requests.post(self.data_url, json = query.to_dict()).json()
      return [self.__create_response_obj(response)]
    else:
      part_variable_code = self.__get_preferred_partition_variable_code_or_default(query)
      part_variable_values = [var["selection"]["values"] for var in query.query if var["code"] == part_variable_code][0]
      # First we check how many values of the partition variable we can include in each request
      part_values_per_request = self.__get_partition_values_per_request(query, part_variable_code)

      # Now we need to fetch the data
      response_list: List[SCBResponse] = []
      for i in range(part_values_per_request, len(part_variable_values) + 1, part_values_per_request):
        part_values = part_variable_values[i-2:i]
        part_query = self.create_query(
          {
            part_variable_code: part_values
          },
          query.response_type
        )
        response = requests.post(self.data_url, json = part_query.to_dict()).json()
        response_list.append(self.__create_response_obj(response))
        sleep(1) # Naively wait 1 sec between requests to make sure we are not too eager. 
        
      return response_list
    
  def create_query(self, variable_selection: dict[str, list] = None, response_type: ResponseType = ResponseType.JSON) -> SCBQuery:
    """
    Params:
      variable_selection: dict
        dict like {"Tid": ["2005M01", "2005M02"], "Alder": ["1", "2", "3", "4", "5"]}. 
          Omitted variable will be using a wildcard, 
            you can specify wildcard as ["*"] if you want to be explicit.
            you can specify a merge as["%"] if you want to group all values within that variable into a single cell.
      response_type: ResponseType
    """
    if not isinstance(response_type, ResponseType):
      raise TypeError("Response type need to be one of type ResponseType, e.g. ResponseType.JSON.")

    query = self.__get_default_query(response_type)
    
    
    if variable_selection != None:
      valid_keys = [var.code for var in self.get_variables()]
      for k, v in variable_selection.items():
        if k not in valid_keys:
          raise KeyError(f"{k} is not a valid variable. These are the valid variables {valid_keys}. For more information visit {self.data_url}.")
        if not isinstance(v, List):
          raise TypeError(f"Got value {v} for key {k}, all values should be lists, even if there is only one element.")
        for i, query_var in enumerate(query.query):
          if query_var["code"] == k:
            if v == ["%"]:
              query.query.pop(i)
            else:
              query_var["selection"]["values"] = v
              query_var["selection"]["filter"] = "item"
    return query

  def get_variables(self) -> List[SCBVariable]:
    """Returns cached variables with possible values if exists, otherwhise fetch, cache and return."""
    if self._variables != None:
      return self._variables
    s = requests.Session()
    response = s.get(self.data_url).json()
    variables = [SCBVariable(var["code"], var["text"], var["values"], var["valueTexts"]) for var in response["variables"]]
    self._variables = variables # cache it
    s.close()
    return variables
  
  def __create_response_obj(self, response_data: dict):
    return SCBResponse(
      columns = response_data["columns"],
      comments = response_data["comments"],
      data = [SCBResponseDataPoint(datapoint["key"], datapoint["values"]) for datapoint in response_data["data"]]
    )

  def __get_default_query(self, response_type: ResponseType) -> SCBQuery:
    """Returns an SCBQuery with all variables defaulted to ["*"]. """
    assert isinstance(response_type, ResponseType)
    scb_query = SCBQuery(
      query = [],
      response_type = response_type
    )
    for var in self.get_variables():
      scb_query.query.append({
        "code": var.code,
        "selection": {
          "filter": "all",
          "values": ["*"]
        }
      })
    return scb_query

  def __get_preferred_partition_variable_code_or_default(self, query: SCBQuery) -> SCBVariable:
    """Returns the preferred partition variable if any, defaults to variable with most values."""
    if self._preferred_partition_variable != None:
      return self._preferred_partition_variable.code
    
    variables = query.query
    variables.sort(key = lambda l: len(l["selection"]["values"]), reverse = True)
    return variables[0]["code"]
  
  def __get_partition_values_per_request(self, query: SCBQuery, part_variable_code: str) -> int:
    """Calculates how many values from the partition variable can be included in the request
    without exceeding SCB limit."""
    values_in_part_variable = [len(var["selection"]["values"]) for var in query.query if var["code"] == part_variable_code][0]
    estimated_count_per_value = self.estimate_cell_count(query) / values_in_part_variable
    values_per_partition = math.floor(SCB_LIMIT_RESULT / estimated_count_per_value)
    return values_per_partition

  @classmethod
  def create_and_validate_client(cls, area, category, category_specification, table):
    """Validates that the area, category, category_specification and table is valid.
        Requires 4 light-weight requests to SCB."""
    s = requests.Session()
    # Validating area
    scb_area_response = s.get(cls.base_url)
    if scb_area_response.status_code != 200:
      raise ConnectionError(f"Couldn't reach SCB at {cls.base_url}")

    if area not in [scb_area["id"] for scb_area in json.loads(scb_area_response.content.decode("latin-1"))]:
      raise ValueError(f"{area} doesn't seem to be a valid area, please visit {cls.base_url} for valid areas.")
   
    _area = area
    
    # Validating category
    scb_category_response = s.get(f"{cls.base_url}/{area}")
    if scb_category_response.status_code != 200:
      raise ConnectionError(f"Couldn't retrieve categories from SCB at {cls.base_url}/{area}")

    if category not in [scb_category["id"] for scb_category in json.loads(scb_category_response.content.decode("latin-1"))]:
      raise ValueError(f"{category} doesn't seem to be a valid category, please visit {cls.base_url}/{area} for valid categories.")

    _category = category

     # Validating category sepcification
    scb_category_specification_response = s.get(f"{cls.base_url}/{area}/{category}")
    if scb_category_specification_response.status_code != 200:
      raise ConnectionError(f"Couldn't retrieve category specifications from SCB at {cls.base_url}/{area}/{category}")

    if category_specification not in [scb_category_spec["id"] for scb_category_spec in json.loads(scb_category_specification_response.content.decode("latin-1"))]:
      raise ValueError(f"{category_specification} doesn't seem to be a valid category specification, please visit {cls.base_url}/{area}/{category} for valid specifications.")

    _category_spec = category_specification

    # Validating table
    scb_table_response = s.get(f"{cls.base_url}/{area}/{category}/{category_specification}")
    if scb_table_response.status_code != 200:
      raise ConnectionError(f"Couldn't retrieve tables from SCB at {cls.base_url}/{area}/{category}/{category_specification}")

    if table not in [scb_table["id"] for scb_table in json.loads(scb_table_response.content.decode("latin-1"))]:
      raise ValueError(f"{category_specification} doesn't seem to be a valid table, please visit {cls.base_url}/{area}/{category}/{category_specification} for valid tables.")

    _table = table

    s.close()

    return SCBClient(
      area = _area,
      category = _category,
      category_specification = _category_spec,
      table = _table
    )