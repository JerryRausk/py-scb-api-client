from SCB_Client import SCBClient, ResponseType
import SCB_Client.SCBClientUtilities as utils
import pandas as pd

def test_csv_to_df():
  scb_client = SCBClient("BE", "BE0101", "BE0101A", "BefolkManad")
  variable_selections = {
    "Kon": ["%"],
    "Region": ["*"],
    "Tid": ["2005M01", "2005M02", "2005M03", "2005M04", "2005M05"]
  }
  scb_client.set_size_limit(0)

  query = scb_client.create_query(
    variable_selection = variable_selections, 
    response_type = ResponseType.CSV
  )
  scb_client.estimate_cell_count(query)
  data = scb_client.get_data(query)
  flattened_data = utils.flatten_data(data)
  expected_datapoints = 31512 # 1(Kon) * 312(Region) * 101(Alder) = 31512
  assert len(flattened_data) == expected_datapoints, "Amount of datapoints doesn't match expected amount."
  df = pd.DataFrame(flattened_data)
  expected_columns = ['region', 'ålder', 'Folkmängden per månad 2005M01', 'Folkmängden per månad 2005M02', 'Folkmängden per månad 2005M03', 'Folkmängden per månad 2005M04', 'Folkmängden per månad 2005M05']
  assert list(df.columns.values) == expected_columns
  expected_df_size = len(expected_columns) * expected_datapoints
  assert df.size == expected_df_size

def test_huge_request():
  """Warning, this will make 150+ requests to SCB and takes a long time to run."""
  scb_client = SCBClient("BE", "BE0101", "BE0101A", "BefolkManad")
  scb_client.set_size_limit(0)
  selected_variables = {
    "Tid": [
      "2000M01", "2000M02", "2000M03", "2000M04", "2000M05", "2000M06",
      "2001M01", "2001M02", "2001M03", "2001M04", "2001M05", "2001M06",
      "2002M01", "2002M02", "2002M03", "2002M04", "2002M05", "2002M06",
      "2003M01", "2003M02", "2003M03", "2003M04", "2003M05", "2003M06"
      ]
  }

  query = scb_client.create_query(
    response_type = ResponseType.CSV,
    variable_selection = selected_variables
  )
  data = scb_client.get_data(query)
  flattened_data = utils.flatten_data(data)
  expected_data_rows = 63024 # 2(Kon) * 312(Region) * 101(Alder)
  expected_data_points_per_row = 27 # Kon, Region, Alder, Tid * 24
  assert len(flattened_data) == expected_data_rows
  assert len(flattened_data[0]) == expected_data_points_per_row