import pytest
from pytest import MonkeyPatch

from SCB_Client import SCBClient, SCBVariable, ResponseType
from SCB_Client.model.scb_models import SCBQuery, SCBQueryVariable, SCBQueryVariableSelection
from SCB_Client.tests.helpers import mock_variables, mock_variables_with_time

def test_wildcard_in_query(monkeypatch: MonkeyPatch):
  expected_query = SCBQuery(
    [
      SCBQueryVariable(
        "first_code",
        SCBQueryVariableSelection(
          "item",
          ["two"]
        )
      ),
      SCBQueryVariable(
        "second_code",
        SCBQueryVariableSelection(
          "item",
          ["four", "five", "six"]
        )
      )
    ],
    ResponseType.JSON
  )
  
  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
  
    variable_selection = {
      "first_code": ["two"],
      "second_code": ["*"]
    }
    query = client.create_query(variable_selection, ResponseType.JSON)
    assert query == expected_query, "Wildcard ['*'] should result in keeping all possible values."
  
def test_omitted_variable(monkeypatch: MonkeyPatch):
  expected_query = SCBQuery(
    [
      SCBQueryVariable(
        "second_code",
        SCBQueryVariableSelection(
          "item",
          ["four", "five"]
        )
      )
    ],
    ResponseType.JSON
  )
  
  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
  
    variable_selection = {
      "first_code": ["%"],
      "second_code": ["four", "five"]
    }
    query = client.create_query(variable_selection, ResponseType.JSON)
    assert query == expected_query, "Variables which are omitted with ['%'] should be popped from query."

def test_no_selection_variables(monkeypatch: MonkeyPatch):
  expected_query = SCBQuery(
    [
      SCBQueryVariable(
        "first_code",
        SCBQueryVariableSelection(
          "item",
          ["one", "two", "three"]
        )
      ),
      SCBQueryVariable(
        "second_code",
        SCBQueryVariableSelection(
          "item",
          ["four", "five", "six"]
        )
      )
    ],
    ResponseType.JSON
  )

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    query = client.create_query(response_type = ResponseType.JSON)
    assert query == expected_query, "We should keep all values in every variable if no selection variables are provided."

def test_default_values_when_creating_query(monkeypatch: MonkeyPatch):
  expected_query = SCBQuery(
    [
      SCBQueryVariable(
        "first_code",
        SCBQueryVariableSelection(
          "item",
          ["one", "two", "three"]
        )
      ),
      SCBQueryVariable(
        "second_code",
        SCBQueryVariableSelection(
          "item",
          ["four", "five", "six"]
        )
      )
    ],
    ResponseType.JSON
  )

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    query = client.create_query()
    assert query == expected_query, "We should keep all values in every variable if no selection variables are provided and response type should be JSON by default."

def test_get_default_query(monkeypatch: MonkeyPatch):
  expected_query = SCBQuery(
    [
      SCBQueryVariable(
        "first_code",
        SCBQueryVariableSelection(
          "item",
          ["one", "two", "three"]
        )
      ),
      SCBQueryVariable(
        "second_code",
        SCBQueryVariableSelection(
          "item",
          ["four", "five", "six"]
        )
      )
    ],
    ResponseType.JSON
  )

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    query = client._SCBClient__get_default_query(ResponseType.JSON)
    assert query == expected_query, "Defualt query factory should return all possible values for every variable."


def test_estimated_cell_counter(monkeypatch: MonkeyPatch):
  expected_count = 9

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    query = client.create_query()
    count = client.estimate_cell_count(query)
    assert count == expected_count, "Estimated cell count should be a product of all the variables in the query."

def test_variable_selection_doesnt_exist(monkeypatch: MonkeyPatch):
  expected_exception = KeyError

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    variable_selection = {
      "wrong_name": ["Stuff"]
    }
    
    with pytest.raises(expected_exception):
      query = client.create_query(variable_selection)

def test_variable_selection_values_must_be_list(monkeypatch: MonkeyPatch):
  expected_exception = TypeError

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    variable_selection = {
      "first_code": "Value"
    }
    
    with pytest.raises(expected_exception):
      query = client.create_query(variable_selection)

def test_response_type_is_not_enum(monkeypatch: MonkeyPatch):
  expected_exception = TypeError

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    
    with pytest.raises(expected_exception):
      query = client.create_query(response_type = "json")

def test_time_top(monkeypatch: MonkeyPatch):
  expected_query = SCBQuery(
    [
      SCBQueryVariable(
        "first_code",
        SCBQueryVariableSelection(
          "item",
          ["one", "two", "three"]
        )
      ),
      SCBQueryVariable(
        "second_code",
        SCBQueryVariableSelection(
          "item",
          ["four", "five", "six"]
        )
      ),
      SCBQueryVariable(
        "third_code",
        SCBQueryVariableSelection(
          "item",
          ["seven", "eigth", "nine", "ten", "eleven", "twelve"]
        )
      ),
      SCBQueryVariable(
        "time_code",
        SCBQueryVariableSelection(
          "item",
          ["2003", "2004", "2005"]
        )
      ),
    ],
    ResponseType.JSON
  )

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables_with_time)
  
    time_top = 3
    query = client.create_query(time_top = time_top)
    assert query == expected_query, "Variables which are omitted with ['%'] should be popped from query."

def test_time_top_is_unavailable(monkeypatch: MonkeyPatch):
  expected_exception = ValueError
  
  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
  
    variable_selection = {
      "first_code": ["two"],
      "second_code": ["*"]
    }
    with pytest.raises(ValueError, match = "Can't find a time variable in current table.") as r:
      client.create_query(time_top = 1)

def test_time_top_and_time_variable_in_selection(monkeypatch: MonkeyPatch):
  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables_with_time)
    variable_selection = {
      "time_code": ["2003", "2004"]
    }
    with pytest.raises(ValueError, match = "Time variable can't be included in variable selection if time_top is used.") as r:
      client.create_query(variable_selection, ResponseType.JSON, 3)
