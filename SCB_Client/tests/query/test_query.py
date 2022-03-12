import pytest
from pytest import MonkeyPatch

from SCB_Client import SCBClient, SCBVariable, ResponseType
from SCB_Client.model.scb_models import SCBQuery, SCBQueryVariable, SCBQueryVariableSelection

def mock_variables():
    return [
      SCBVariable(
        "first_code",
        "first_code",
        ["one", "two", "three"],
        ["one", "two", "three"]
      ),
      SCBVariable(
        "second_code",
        "second_code",
        ["four", "five", "six"],
        ["four", "five", "six"]
      )
    ]

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
