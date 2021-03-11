# Standardized Data IO API

XXX Provide motivation and similar packages, like Spark Optimus

## Credential Cacheing

In its current instantiation, credentials are stored as individual secrets. Each backend will require its own set credentials including a `username` and a `password`. Future iterations will use a single, consolidated configuration (INI) file to make things easier.

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

# Cache username and password for back end
# Note that user@ is intentional. More on this later
databricks secrets put --scope <first name>.<last name> --key "redshift://user@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com/rscdwdm-username"
databricks secrets put --scope <first name>.<last name> --key "redshift://user@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com/rscdwdm-password"

# A second example with an Oracle database
databricks secrets put --scope <first name>.<last name> --key "oracle://user@CDWPRD-rac-db.thermo.com/cdwprd_users-username"
databricks secrets put --scope <first name>.<last name> --key "oracle://user@CDWPRD-rac-db.thermo.com/cdwprd_users-password"
```

## Connecting to Data

The easiest way to connect to data is to leverage `build_connection` and a fully-qualified URL. Examples below.

### Connecting to a Database: Single Source

Reading from a single source (e.g., a table) in a database is straightforward. Specify the URL and invoke the `read` method.

```
# Example using Redshift data source
url = 'redshift://user@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscdwdm.lsgds.sf_db_data__c'

connection = build_connection(url)

# Returns a PySpark dataframe
data = connection.read()

# Equivalent call
data = connection.read(query="SELECT * FROM lsgds.sf_db_data__c")
```

### Connecting to a Database: Multiple Sources

Combining multiple sources from a single backend is a common use case. For example, the user may want to combine (join) information across multiple tables housed in the same database. This is fully supported through the API.

```
# Connect to the database
url = 'redshift://user@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscdwdm'

# Execute SQL
connection = build_connection(url)

data = connection.read(query="Your Awesome SQL query")
```

Alternatively, an identical result can be achieved by executing a SQL query through a connection to one of the tables in the database.

## Other Examples

```
# Oracle
url = 'oracle://user@CDWPRD-rac-db.thermo.com:1521/cdwprd_users'
connection = build_connection(url)
data = connection.read('SELECT * FROM CDWREAD.T_PB')
```

```
# S3
url = 's3a://tfsds-lsg-test/model-output-archive/backup/DrugDiscovery_txns'
connection = build_connection(url)
data = connection.read()
```
