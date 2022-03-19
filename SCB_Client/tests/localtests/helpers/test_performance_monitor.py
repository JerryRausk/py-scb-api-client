import uu
from SCB_Client.SCBClientUtilities import PerformanceMonitor, SessionType
from time import sleep
import pytest

def test_not_implemented_session_type():
  monitor = PerformanceMonitor()
  uuid = monitor.start_session(SessionType)
  with pytest.raises(NotImplementedError):
    monitor.stop_session(uuid)

def test_process_session():
  monitor = PerformanceMonitor()
  uuid = monitor.start_session(SessionType.PROCESS)
  sleep(0.3)
  td = monitor.stop_session(uuid)
  assert 200000 < td.microseconds < 400000
  
def test_download_session():
  monitor = PerformanceMonitor()
  uuid = monitor.start_session(SessionType.DOWNLOAD)
  sleep(0.3)
  td = monitor.stop_session(uuid)
  assert 200000 < td.microseconds < 400000

def test_correct_session_is_stopped():
  monitor = PerformanceMonitor()
  uuid_to_stop = monitor.start_session(SessionType.DOWNLOAD)
  sleep(0.3)
  uuid_to_continue = monitor.start_session(SessionType.PROCESS)
  td = monitor.stop_session(uuid_to_stop)
  assert 200000 < td.microseconds < 400000
  assert uuid_to_continue in [ses["uuid"] for ses in monitor._PerformanceMonitor__ongoing_sessions]
