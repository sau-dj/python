This Python code automates downloading COVID-19 data from a GitHub repository and uploading it to a SQLite database named "covid_data.db". It also creates a Flask web application that allows users to access this data. The application provides two functionalities:

Get statistics by country: The /country route retrieves data grouped by country (confirmed cases, deaths, recovered) sorted by update date.
Get global summary (optional filter): The /summary route provides a global summary of total confirmed cases, deaths, and recovered. Users can optionally specify start and end dates to filter the results.


![image](https://github.com/sau-dj/python/blob/main/Covid%20ETL%20Project/etlproject.png)
