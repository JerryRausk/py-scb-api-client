from typing import List
from datetime import timedelta, datetime
from uuid import UUID, uuid4
from enum import Enum


class SessionType(Enum):
  DOWNLOAD = "download"
  PROCESS = "process"

class PerformanceMonitor():
  download_sessions: List[timedelta] = []
  process_session: List[timedelta] = []
  __ongoing_sessions: List[dict] = []

  def start_session(self, type: SessionType) -> UUID:
    new_uuid = uuid4()
    now = datetime.now()
    self.__ongoing_sessions.append(
      {
        "uuid": new_uuid,
        "time": now,
        "type": type
      }
    )
    return new_uuid
  
  def stop_session(self, uuid: UUID) -> timedelta:
    session_to_stop = [session for session in self.__ongoing_sessions if session["uuid"] == uuid][0]
    if not session_to_stop:
      raise KeyError("No found session for {uuid}.")
    td = datetime.now() - session_to_stop["time"]
    if session_to_stop["type"] == SessionType.DOWNLOAD:
      self.download_sessions.append(td)
    elif session_to_stop["type"] == SessionType.PROCESS:
      self.process_session.append(td)
    else:
      raise NotImplementedError("This sessions type is not recognized.")
    self.download_sessions.append(td)
    self.__ongoing_sessions.remove(session_to_stop)
    return td
    

def flatten_data(nested_data: List[list]) -> list:
  return [item for sublist in nested_data for item in sublist]