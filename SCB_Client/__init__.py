import json
import math
from time import sleep
from typing import List, Optional
import requests

from SCB_Client.model.scb_models import (ResponseType, SCBJsonResponse,
                                         SCBJsonResponseDataPoint, SCBQuery,
                                         SCBQueryVariable,
                                         SCBQueryVariableSelection,
                                         SCBVariable)


class SCBClient:
  _SCB_BASE_URL: str = "https://api.scb.se/OV0104/v1/doris/sv/ssd"
  _SCB_LIMIT_RESULT: int = 150000
  _variables: List[SCBVariable] = None # Used to cache variableas in case they are needed multiple times
  _size_limit_cells: int = 30000
  _preferred_partition_variable_code: str = None

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
    self.data_url = f"{self._SCB_BASE_URL}/{area}/{category}/{category_specification}/{table}"

  def set_preferred_partition_variable_code(self, variable_code: str) -> None:
    """preferred_partition_variable_code will be used to partition the requests if the expected result is larger than the SCB limit."""
    valid_variables = [var.code for var in self.get_variables()]
    if variable_code not in valid_variables:
      raise KeyError(f"{variable_code} is not a valid variable, the valid variables are {', '.join(valid_variables)}")
    self._preferred_partition_variable_code = variable_code

  def get_preferred_partition_variable_code(self) -> SCBVariable:
    """preferred_partition_variable_code will be used to partition the requests if the expected result is larger than the SCB limit."""
    return self._preferred_partition_variable_code

  def set_size_limit(self, limit: int) -> None:
    """Size limit is used to protect against unwanted data use, will be ignored if set to 0."""
    if not isinstance(limit, int) or limit < 0:
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

    return math.prod([len(queryvar.selection.values) for queryvar in query.query])

  def get_data(self, query: SCBQuery) -> List[SCBJsonResponse]:
    """
    Get data from SCB if internal limit is not exceeded. 
    Multiple requests will be made if the SCB limit is exceeded.
    """
    estimated_cell_count = self.estimate_cell_count(query)
    if estimated_cell_count > self._size_limit_cells and self._size_limit_cells > 0:
      raise PermissionError(f"Current size limit {self._size_limit_cells} will be exceeded. The size limit can be changed with set_size_limit().")
    if estimated_cell_count < self._SCB_LIMIT_RESULT:
      response = requests.post(self.data_url, json = query.to_dict())
      return [self.__create_response_obj(response, query.response_type)]
    else:
      partition_variable = self.__get_preferred_partition_variable_or_default(query)
      
      # First we check how many values of the partition variable we can include in each request
      partition_values_per_request = self.__get_partition_values_per_request(query, partition_variable)

      # Now we need to fetch the data
      values_to_partition = partition_variable.selection.values.copy()
      partitions = self.__partition_list(values_to_partition, partition_values_per_request)
      response_list: List[SCBJsonResponse] = []
      for partition in partitions:
        partition_variable.selection.values = partition

        # TODO: This is horrible
        fetched = False
        while True:
          response = requests.post(self.data_url, json = query.to_dict())
          if response.status_code == 429:
            sleep(0.1)
            continue
          else:
            break
          
        response_list.append(self.__create_response_obj(response, query.response_type))
        
      return response_list
  
  def create_query(self, variable_selection: Optional[dict[str, list]] = None, response_type: ResponseType = ResponseType.JSON, time_top: int = 0) -> SCBQuery:
    """
    Params:
      variable_selection: dict
        dict like {"Tid": ["2005M01", "2005M02"], "Alder": ["1", "2", "3", "4", "5"]}. 
          Omitted variable will be using a wildcard, 
            you can specify wildcard as ["*"] if you want to be explicit.
            you can specify a merge as["%"] if you want to group all values within that variable into a single cell.
      response_type: ResponseType = ResponseType.JSON
      time_top: int = 0
        Will raise a ValueError if none of the variables available in current table is a time variable.
    """

    # Validating input
    if not isinstance(response_type, ResponseType):
      raise TypeError("Response type need to be one of type ResponseType, e.g. ResponseType.JSON.")
    if not isinstance(time_top, int) or time_top < 0:
      raise TypeError("time_top should be a positive integer.")
    if variable_selection != None and not isinstance(variable_selection, dict):
      raise TypeError("Variable selection needs to be a dictionary.")

    query = self.__get_default_query(response_type)

    # Handling variable selection
    if variable_selection != None:
      valid_keys = query.query_variable_codes_to_list()
      for k, v in variable_selection.items():
        if k not in valid_keys:
          raise KeyError(f"{k} is not a valid variable. These are the valid variables {valid_keys}. For more information visit {self.data_url}.")
        if not isinstance(v, List):
          raise TypeError(f"Got value {v} for key {k}, all values should be lists, even if there is only one element.")
        for var in query.query:
          if var.code == k:
            if v == ["%"]:
              query.query.remove(var)
            elif v == ["*"]:
              pass # default query already has all the values listed, same as ["*"]
            else:
              var.selection.values = v
    
    # Handling time top selection
    if time_top > 0:
      time_variable = [var for var in self.get_variables() if var.time == True]
      if not time_variable:
        raise ValueError("Can't find a time variable in current table.")
      time_variable = time_variable[0]
      time_query_variable = [qv for qv in query.query if qv.code == time_variable.code][0]
      if variable_selection != None and time_query_variable.code in variable_selection.keys():
        raise ValueError("Time variable can't be included in variable selection if time_top is used.")
      time_values_count = len(time_query_variable.selection.values)
      time_query_variable.selection.values = time_query_variable.selection.values[time_values_count - time_top : time_values_count]

    return query

  def get_variables(self) -> List[SCBVariable]:
    """Returns cached variables with possible values if exists, otherwhise fetch, cache and return."""
    if self._variables != None:
      return self._variables
    s = requests.Session()
    response = s.get(self.data_url).json()
    variables = [SCBVariable(**var) for var in response["variables"]]
    self._variables = variables # cache it
    s.close()
    return variables

  @staticmethod
  def __partition_list(list_to_partition: list, partition_size: int) -> List[list]:
    list_partitioned = []
    for i in range(0, len(list_to_partition), partition_size):
      list_partitioned.append(list_to_partition[i:i+partition_size])
    return list_partitioned

  @staticmethod
  def __create_response_obj(response_data: requests.Response, response_type: ResponseType):
    if response_type == ResponseType.JSON:
      json_data = response_data.json()
      return SCBJsonResponse(
        columns = json_data["columns"],
        comments = json_data["comments"],
        data = [
          SCBJsonResponseDataPoint(
            datapoint["key"], 
            datapoint["values"]
          ) 
          for datapoint 
          in json_data["data"]
        ]
      )
    
    elif response_type == ResponseType.CSV:
      content = response_data.content.decode("latin-1").replace("\"", "").replace("'", "")
      lines = content.strip().split("\r\n")
      headers = lines[0].split(",")
      datapoints = []
      for line in lines[1:]:
        l = line.strip()
        datapoint = {}
        for i, value in enumerate(l.split(",")):
          datapoint[headers[i]] = value
        datapoints.append(datapoint)
      return datapoints
      
    
    else:
      raise NotImplementedError("This response type is not supported yet.")

  def __get_default_query(self, response_type: ResponseType) -> SCBQuery:
    """Returns an SCBQuery with all variables defaulted to ["*"]. """
    assert isinstance(response_type, ResponseType)
    scb_query = SCBQuery(
      query = [],
      response_type = response_type
    )
    for var in self.get_variables():
      scb_query.query.append(SCBQueryVariable(
        var.code,
        SCBQueryVariableSelection(
          "item",
          var.values
        )
      ))
    return scb_query

  def __get_preferred_partition_variable_or_default(self, query: SCBQuery) -> SCBQueryVariable:
    """Returns the preferred partition variable if any, defaults to variable in query with most values."""
    if self._preferred_partition_variable_code != None:
      return [queryvar for queryvar in query.query if queryvar.code == self._preferred_partition_variable_code][0]
    
    variables = query.query
    variables.sort(key = lambda qv: len(qv.selection.values), reverse = True)
    return variables[0]
  
  def __get_partition_values_per_request(self, query: SCBQuery, partition_variable: SCBQueryVariable) -> int:
    """Calculates how many values from the partition variable can be included in the request
    without exceeding SCB limit."""
    estimated_count_per_value = self.estimate_cell_count(query) / len(partition_variable.selection.values)
    if estimated_count_per_value > self._SCB_LIMIT_RESULT:
      raise ValueError(f"Can't partition by {partition_variable.code}, each partition would exceed SCB limit ({int(estimated_count_per_value)} > {self._SCB_LIMIT_RESULT}).")
    values_per_partition = math.floor(self._SCB_LIMIT_RESULT / estimated_count_per_value)
    return values_per_partition

  @classmethod
  def create_and_validate_client(cls, area: str, category: str, category_specification: str, table: str):
    """Validates that the area, category, category_specification and table is valid.
        Requires 4 light-weight requests to SCB."""
    s = requests.Session()
    # Validating area
    scb_area_response = s.get(cls._SCB_BASE_URL)
    if scb_area_response.status_code != 200:
      raise ConnectionError(f"Couldn't reach SCB at {cls._SCB_BASE_URL}")

    if area not in [scb_area["id"] for scb_area in json.loads(scb_area_response.content.decode("latin-1"))]:
      raise ValueError(f"{area} doesn't seem to be a valid area, please visit {cls._SCB_BASE_URL} for valid areas.")
   
    _area = area
    
    # Validating category
    scb_category_response = s.get(f"{cls._SCB_BASE_URL}/{area}")
    if scb_category_response.status_code != 200:
      raise ConnectionError(f"Couldn't retrieve categories from SCB at {cls._SCB_BASE_URL}/{area}")

    if category not in [scb_category["id"] for scb_category in json.loads(scb_category_response.content.decode("latin-1"))]:
      raise ValueError(f"{category} doesn't seem to be a valid category, please visit {cls._SCB_BASE_URL}/{area} for valid categories.")

    _category = category

     # Validating category sepcification
    scb_category_specification_response = s.get(f"{cls._SCB_BASE_URL}/{area}/{category}")
    if scb_category_specification_response.status_code != 200:
      raise ConnectionError(f"Couldn't retrieve category specifications from SCB at {cls._SCB_BASE_URL}/{area}/{category}")

    if category_specification not in [scb_category_spec["id"] for scb_category_spec in json.loads(scb_category_specification_response.content.decode("latin-1"))]:
      raise ValueError(f"{category_specification} doesn't seem to be a valid category specification, please visit {cls._SCB_BASE_URL}/{area}/{category} for valid specifications.")

    _category_spec = category_specification

    # Validating table
    scb_table_response = s.get(f"{cls._SCB_BASE_URL}/{area}/{category}/{category_specification}")
    if scb_table_response.status_code != 200:
      raise ConnectionError(f"Couldn't retrieve tables from SCB at {cls._SCB_BASE_URL}/{area}/{category}/{category_specification}")

    if table not in [scb_table["id"] for scb_table in json.loads(scb_table_response.content.decode("latin-1"))]:
      raise ValueError(f"{category_specification} doesn't seem to be a valid table, please visit {cls._SCB_BASE_URL}/{area}/{category}/{category_specification} for valid tables.")

    _table = table

    s.close()

    return SCBClient(
      area = _area,
      category = _category,
      category_specification = _category_spec,
      table = _table
    )