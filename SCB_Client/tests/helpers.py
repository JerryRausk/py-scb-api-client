from SCB_Client.model.scb_models import SCBVariable


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

def mock_variables_with_time():
  return [
    SCBVariable(
      "first_code",
      "first_text",
      ["one", "two", "three"],
      ["one", "two", "three"]
    ),
    SCBVariable(
      "second_code",
      "second_text",
      ["four", "five", "six"],
      ["four", "five", "six"]
    ),
    SCBVariable(
      "third_code",
      "third_text",
      ["seven", "eigth", "nine", "ten", "eleven", "twelve"],
      ["seven", "eigth", "nine", "ten", "eleven", "twelve"],
      False,
      False
    ),
    SCBVariable(
      "time_code",
      "time_text",
      ["2000", "2001", "2002", "2003", "2004", "2005"],
      ["2000", "2001", "2002", "2003", "2004", "2005"],
      False,
      True
    )
  ]