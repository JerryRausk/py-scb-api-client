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

def test_sum_one_session_type():
  monitor = PerformanceMonitor()
  first_uuid = monitor.start_session(SessionType.DOWNLOAD)
  sleep(0.3)
  first_td = monitor.stop_session(first_uuid)
  second_uuid = monitor.start_session(SessionType.DOWNLOAD)
  sleep(0.2)
  second_td = monitor.stop_session(second_uuid)
  total_microseconds = first_td.microseconds + second_td.microseconds

  uuid_to_not_include = monitor.start_session(SessionType.PROCESS)
  td = monitor.stop_session(uuid_to_not_include)
  
  allowed_difference_microseconds = 1000
  assert (
    total_microseconds - allowed_difference_microseconds 
    <= monitor.total_session_time_microseconds(SessionType.DOWNLOAD) 
    <= total_microseconds + allowed_difference_microseconds
  )