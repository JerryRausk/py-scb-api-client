from pytest import MonkeyPatch
from SCB_Client import SCBClient
import requests
from SCB_Client.SCBClientUtilities import SessionType
import time
import pytest

class mocked_response():
  def __init__(self, status_code):
    self.status_code = status_code
    self.content = b'[{"id": "Mocked_response"}]'

def mocked_successful_get(url: str):
  time.sleep(0.005)
  return mocked_response(200)

def mocked_unsuccessful_get(url: str):
  time.sleep(0.005)
  return mocked_response(404)

def test_performance_monitor_can_be_injected(monkeypatch: MonkeyPatch):
  with monkeypatch.context() as m:
    mocked_session = requests.session()
    m.setattr(mocked_session, "get", mocked_successful_get)
    new_client = SCBClient.create_and_validate_client(
      "Mocked_response",
      "Mocked_response",
      "Mocked_response",
      "Mocked_response",
      session = mocked_session
    )
    assert new_client.perf_mon.total_session_time_microseconds(SessionType.DOWNLOAD) > 0

def test_creation_is_aborted_if_unkown_metadata(monkeypatch: MonkeyPatch):
  with monkeypatch.context() as m:
    mocked_session = requests.session()
    m.setattr(mocked_session, "get", mocked_successful_get)
    with pytest.raises(ValueError):
      new_client = SCBClient.create_and_validate_client(
        "Mocked_unkown_response",
        "Mocked_unkown_response",
        "Mocked_unkown_response",
        "Mocked_unkown_response",
        session = mocked_session
      )

def test_creation_aborts_on_connection_error(monkeypatch: MonkeyPatch):
  with monkeypatch.context() as m:
    mocked_session = requests.session()
    m.setattr(mocked_session, "get", mocked_unsuccessful_get)
    with pytest.raises(ConnectionError):
      new_client = SCBClient.create_and_validate_client(
        "Mocked_response",
        "Mocked_response",
        "Mocked_response",
        "Mocked_response",
        session = mocked_session
      )