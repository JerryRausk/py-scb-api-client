import pytest
from pytest import MonkeyPatch

from SCB_Client.tests.helpers import mock_variables
from SCB_Client import SCBClient, SCBVariable, ResponseType
from SCB_Client.model.scb_models import SCBQuery, SCBQueryVariable, SCBQueryVariableSelection

def test_set_preferred_partitioning_variable(monkeypatch: MonkeyPatch):
  preferred_partition_variable = "first_code"
  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    client.set_preferred_partition_variable_code(preferred_partition_variable)
    assert client._preferred_partition_variable_code == preferred_partition_variable, "We should be able to set a valid preferred partition variable."

def test_set_unknown_partitioning_variable(monkeypatch: MonkeyPatch):
  preferred_partition_variable = "unknown"  
  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    with pytest.raises(KeyError):
      client.set_preferred_partition_variable_code(preferred_partition_variable)

def test_partition_values_per_request_with_preferred(monkeypatch: MonkeyPatch):
  preferred_partition_variable_code = "first_code"
  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    query = client.create_query()

    client.set_preferred_partition_variable_code(preferred_partition_variable_code)
    preferred_partition_variable = [var for var in query.query if var.code == preferred_partition_variable_code][0]
    expect_values_per_req = client._SCB_LIMIT_RESULT / len(preferred_partition_variable.selection.values)
    values_per_req = client._SCBClient__get_partition_values_per_request(query, preferred_partition_variable)
    assert values_per_req == expect_values_per_req
    # Drop limit to 5, 5/3 = 1,66
    client._SCB_LIMIT_RESULT = 5
    values_per_req = client._SCBClient__get_partition_values_per_request(query, preferred_partition_variable)
    assert values_per_req == 1
    # Increase limit to 6, 6/3 = 2
    client._SCB_LIMIT_RESULT = 6
    values_per_req = client._SCBClient__get_partition_values_per_request(query, preferred_partition_variable)
    assert values_per_req == 2

def test_cant_partition_enough(monkeypatch: MonkeyPatch):
  preferred_partition_variable_code = "first_code"
  expected_exception = ValueError

  with monkeypatch.context() as m:
    client = SCBClient(
      "Test",
      "Test",
      "Test",
      "Test"
    )
    m.setattr(client, "get_variables", mock_variables)
    query = client.create_query()

    client.set_preferred_partition_variable_code(preferred_partition_variable_code)
    preferred_partition_variable = [var for var in query.query if var.code == preferred_partition_variable_code][0]

    # Decrease limit to be below what is possible
    client._SCB_LIMIT_RESULT = 2
    with pytest.raises(expected_exception):
      client._SCBClient__get_partition_values_per_request(query, preferred_partition_variable)