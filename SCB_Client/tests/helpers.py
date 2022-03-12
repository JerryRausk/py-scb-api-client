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