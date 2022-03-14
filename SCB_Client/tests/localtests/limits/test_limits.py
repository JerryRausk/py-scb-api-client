import pytest
from SCB_Client import SCBClient

def test_default_internal_limit():
  client = SCBClient(
    "Test",
    "Test",
    "Test",
    "Test"
  )
  assert client._size_limit_cells == 30000

def test_set_new_internal_limit_to_zero():
  client = SCBClient(
    "Test",
    "Test",
    "Test",
    "Test"
  )
  client.set_size_limit(0)
  assert client._size_limit_cells == 0

def test_set_new_internal_limit():
  client = SCBClient(
    "Test",
    "Test",
    "Test",
    "Test"
  )
  client.set_size_limit(200000)
  assert client._size_limit_cells == 200000

def test_set_internal_limit_to_string():
  client = SCBClient(
    "Test",
    "Test",
    "Test",
    "Test"
  )
  with pytest.raises(ValueError):
    client.set_size_limit("200000")
  
def test_set_internal_limit_to_negative():
  client = SCBClient(
    "Test",
    "Test",
    "Test",
    "Test"
  )
  with pytest.raises(ValueError):
    client.set_size_limit(-3000)