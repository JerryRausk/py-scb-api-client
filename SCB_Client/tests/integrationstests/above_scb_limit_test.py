from SCB_Client import SCBClient, ResponseType, SCBClientUtilities as utils
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

  query = scb_client.create_query(
    response_type = ResponseType.CSV
  )
  data = scb_client.get_data(query)
  pass