# SCB_Client
Adds a layer of abstraction to SCB (Statistic Sweden) API.

## Get started
Download or clone the repo into your project folder, then it is as simple as importing the client and specifying which tables data you want to fetch. 

```Python
from SCB_Client import SCBClient, ResponseType


# Initialize the client, in this example we are using population data.
scb_client = SCBClient.create_and_validate_client("BE", "BE0101", "BE0101A", "BefolkManad") 
scb_client.set_size_limit(0) # Unlimited response size, defaults to 30k
scb_client.set_preferred_partition_variable_code("Alder") # You can specify which variable you want to partition the result by if result exceeds SCB limit
# Create a dict of variables to be used in query
variable_selections = {
  # Support specifying which values of the variable you want to include
  "Tid": ["2001M01", "2001M02", "2001M03", "2001M04", "2001M05"],
  # Supports omitting variables with % if you don't want the data to be grouped by that 
  "Alder": ["%"], variable
  # Supports including all available values of a variable with * (defualt behaviour if variable is not specifyed in query).
  "Kon": ["*"] 
}

# Create the query
query = scb_client.create_query(variable_selections, ResponseType.JSON)

# Fetch the data
scb_data = scb_client.get_data(query)
```