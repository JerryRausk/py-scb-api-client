from SCB_Client import SCBClient, ResponseType

def test_json_response():
  scb_client = SCBClient("BE", "BE0101", "BE0101A", "BefolkManad")
  variable_selections = {
    "Alder": ["18", "25", "30", "35", "40"],
    "Kon": ["%"],
    "Region": ["*"] 
  }

  no_of_time_units_to_include = 5

  query = scb_client.create_query(
    variable_selection = variable_selections, 
    response_type = ResponseType.JSON,
    time_top = no_of_time_units_to_include
  )

  data = scb_client.get_data(query)
  expected_columns = [
    {
      'code': 'Region',
      'text': 'region', 
      'type': 'd'
    }, 
    {
      'code': 'Alder', 
      'text': 'ålder', 
      'comment': 'Med ålder avses uppnådd ålder vid årets slut, d.v.s. i princip en redovisning efter födelseår.\r\n', 
      'type': 'd'
    }, 
    {
      'code': 'Tid', 
      'text': 'månad', 
      'type': 't'
    }
      , 
    {
      'code': '000003O5', 
      'text': 'Folkmängden per månad', 
      'type': 'c'
    }
  ]
  assert data[0].columns == expected_columns
  assert len(data[0].data) == 7800

def test_csv_response():
  scb_client = SCBClient("BE", "BE0101", "BE0101A", "BefolkManad")
  variable_selections = {
    "Alder": ["18", "25", "30", "35", "40"],
    "Kon": ["%"],
    "Region": ["*"],
    "Tid": ["2005M01", "2005M02", "2005M03", "2005M04", "2005M05"]
  }

  query = scb_client.create_query(
    variable_selection = variable_selections, 
    response_type = ResponseType.CSV
  )

  data = scb_client.get_data(query)
  expected_datapoints = 1560 # 5(Alders) * 312(Region)
  assert len(data[0]) == expected_datapoints, "Amount of datapoints doesn't match expected amount."

  expected_first_observation = {
    'region': '00 Riket', 
    'ålder': '18 år', 
    'Folkmängden per månad 2005M01': '112989', 
    'Folkmängden per månad 2005M02': '113013', 
    'Folkmängden per månad 2005M03': '113046', 
    'Folkmängden per månad 2005M04': '113064', 
    'Folkmängden per månad 2005M05': '113083'
  }
  assert data[0][0] == expected_first_observation, "First observation doesn't match expected result."