# Standardized Data IO API

This is a companion package intended to be used within Databricks. Each individual team has its own solution to standardized data reading/writing, but the APIs are awkward and backend-specific. This package was born out of our need for a standardized data IO API.

This package standardizes data read/write via spark. It has overlapping scope with other projets such as Spark Optimus, but is hyper focused on URL-based data IO in Spark.


The package currently supports R/W in S3, Redshift, and Oracle. Extending the package to other backends is straightforward.

## Installation in Databricks

Install the package as an `egg` in the desired cluster. These will be provided or can by built using standard Python utilities.

```
# Example of building egg locally
python .\setup.py bdist_egg
```

## Credential Cacheing

In its current instantiation, credentials are stored in a `credentials.cfg` file in a user-specific scope in Databricks. The file follows standard INI formatting.


Secrets will be stored in a user-specific scope. Below are example commands for a single Redshift database. These commands should be run on your **local** computer using the databricks cli.

```
# Run this if this is your first time, run this in a terminal
# For Windoze users, run this in PowerShell
# For others, you know what to do already :)

# Install the client
pip install databricks-cli

# Create user scope
# Note that the name must match your Thermo Fisher user name/login credentails
databricks secrets create-scope --scope <first name>.<last name>
```

Next, generate a single file on your *local* machine that mimicks the following.

```
[<scheme>://user@<hostname>/<path>]
username = <username>
password = <password>
```

This file must then be uploaded as a secret to the scope created above.

```
databricks secrets put --scope <first name>.<last name> --key "credentials.cfg"
```

The underlying builders will reference these credentials when appropriate. Note that S3 credentials are currently only supported through Databricks cluster roles. This needs to be improved.

## Connecting to Data

The easiest way to connect to data is to leverage `build_connection` and a fully-qualified URL. Examples below.

### Connecting to a Database: Single Source

#### Read
Reading from a single source (e.g., a table) in a database is straightforward. Specify the URL and invoke the `read` method.

```
# Example using Redshift data source
url = 'redshift://user@<hostname>:<port>/<database>.<schema>.<table>'

connection = build_connection(url)

# Returns a PySpark dataframe
data = connection.read()

# Equivalent call
data = connection.read(query="SELECT * FROM <schema>.<table>")
```

#### Write

Data can be written to a single location, such as an S3 bucket or Redshift table. Examples below.

```
from data_io import build_connection
import pandas as pd

data = pd.DataFrame(dict(a=1, b=2), index=[0])

data = spark.createDataFrame(data)

# And let's try redshift now
url = 'redshift://user@<hostname>:<port>/<database>.<schema>.<table>'

connection = build_connection(url)

connection.write(data)
```

#### Custom Options

Each connection has a set of default options. These can be overridden using keyword arguments to read and write methods.

For example, to change the write mode simply provide the desired write mode.

```
connection.write(data, mode='append')
```

### Connecting to a Database: Multiple Sources

Combining multiple sources from a single backend is a common use case. For example, the user may want to combine (join) information across multiple tables housed in the same database. This is fully supported through the API.

```
# Connect to the database
url = 'redshift://user@<hostname>:<port>/<database>

# Execute SQL
connection = build_connection(url)

data = connection.read(query="Your Awesome SQL Query")
```

Alternatively, an identical result can be achieved by executing a SQL query through a connection to one of the tables in the database.

## Other Examples

```
# Oracle
url = 'oracle://user@<hostname>:<port>/<SID>'
connection = build_connection(url)
data = connection.read('SELECT * FROM <SID>.<table>')
```

```
# S3
url = 's3a://<bucket>/<file>'
connection = build_connection(url)
data = connection.read()
```
