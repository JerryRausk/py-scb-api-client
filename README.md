# SCB_Client
Adds a layer of abstraction to SCB (Statistic Sweden) API.

## Get started
Download or clone the repo into your project folder, then it is as simple as importing the client and specifying which tables data you want to fetch. 

```Python
from SCB_Client import SCBClient, ResponseType


# Initialize the client, in this example we are using population data.
scb_client = SCBClient.create_and_validate_client("BE", "BE0101", "BE0101A", "BefolkManad") 

# 0 is unlimited, defaults to 30k
scb_client.set_size_limit(0) 

# You can specify which variable you want to partition the result by if result exceeds SCB limit, defaults to the variable with most values.
scb_client.set_preferred_partition_variable_code("Alder") 

# Create a dict of variables to be used in query
variable_selections = {
  # Support specifying which values of the variable you want to include
  "Alder": ["18", "25", "30", "35", "40"],
  # Supports omitting variables with % if you don't want the data to be grouped by that 
  "Kon": ["%"],
  # Supports including all available values of a variable with * (this is the defualt behaviour if variable is not specifyed in query).
  "Region": ["*"] 
}

# We can specify number of values of the time variable (if exists) we would like to include by using time_top
no_of_time_units_to_include = 5

# Create the query
query = scb_client.create_query(
  variable_selection = variable_selections, 
  response_type = ResponseType.JSON,
  time_top = no_of_time_units_to_include)

# We can estimate no of cells that will be downloaded.
# In this particular case it results in 7800 data points, 312(Region) * 5(Alder) * 1(Kon) * 5(time units)
print(scb_client.estimate_cell_count(query))

# Finally we fetch the data.
# If the data exceeds SCB's limit on results per request this will result in multiple requests.
scb_data = scb_client.get_data(query)
```

## With Pandas
In this example we are requesting a CSV response, this is recommended since it will significantly smaller than the json equivalent and the result ends up in a DataFrame anyway.
```Python
from SCB_Client import SCBClient, ResponseType, SCBClientUtilities as utils
import pandas as pd

scb_client = SCBClient("BE", "BE0101", "BE0101A", "BefolkManad")
  variable_selections = {
    "Kon": ["%"],
    "Region": ["*"],
    "Tid": ["2005M01", "2005M02", "2005M03", "2005M04", "2005M05"]
  }
  scb_client.set_size_limit(200000)

  query = scb_client.create_query(
    variable_selection = variable_selections, 
    response_type = ResponseType.CSV
  )
  # This is about 157k "cells" and will be done in 2 requests.
  data = scb_client.get_data(query)

  # We flatten the list of lists to a single list
  flattened_data = utils.flatten_data(data)

  # The flattened list can be passed to pandas.DataFrame.
  df = pd.DataFrame(flattened_data)
```