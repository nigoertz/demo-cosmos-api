# What is this?
This api is needed to connect to using the erp-connector-demo with the api strategy.
U need to create the following .env file and use your cosmosdb credentials as well as the monitoring frontend url:
# create .env-file
```
COSMOSDB_URL=something.cosmos.azure.com
COSMOSDB_USER=myusername
COSMOSDB_PASSWORD=some-cosmosdb-password
MONITORING_URL=http://url:port
```
# Run
After that you can build and run the dockerfile:

`docker build -t <tagname> .`

`docker run -p 8000:8000 --env-file .env <tagname>`
