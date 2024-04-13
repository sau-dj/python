import sqlite3  # Library for connecting to the database
import json  # Library for handling JSON data
from flask import Flask  # Library for creating Flask application
from flask import request  # Library for handling GET request parameters

db_name = 'covid_data'  # Name of the SQLite database
app = Flask(__name__)  # Creating Flask application instance

app.config["DEBUG"] = True  # Setting Flask application to run in debug mode

@app.route('/country', methods=['GET'])  # Route for retrieving covid statistics per country
def country():
    """Retrieve covid statistics per country"""
    conn = sqlite3.connect(f"{db_name}.db")  # Connecting to the SQLite database
    c = conn.cursor()  # Creating a cursor object
    # Executing SQL query to get covid statistics per country
    c.execute(
        """
         SELECT country_region, SUM(Confirmed), DATE(last_update) AS last_update 
         FROM covid_data GROUP BY country_region, last_update 
         ORDER BY last_update;

     """)
    c.execute(
        """select Country_Region, sum(Confirmed), sum(deaths), sum(recovered) 
        from covid_data 
        group by Country_Region 
        order by country_region;""")
    content = c.fetchall()  # Fetching the query result
    c.close()  # Closing the cursor
    # Constructing response data
    data = [{'country': country, 'confirmed': confirmed, 'deaths': deaths, 'recovered': recovered} \
            for country, confirmed, deaths, recovered in content]
    return {
        'request_data': data  # Returning the response data
    }


@app.route('/summary', methods=['GET'])  # Route for retrieving global summary of covid statistics
def summary():
    """Retrieve global summary of covid statistics"""
    conn = sqlite3.connect(f"{db_name}.db")  # Connecting to the SQLite database
    c = conn.cursor()  # Creating a cursor object
    # Optional parameters in format YYYY-MM-DD, ex 2020-09-13
    start = request.args.get('start')  # Retrieving start date parameter from request
    end = request.args.get('end')  # Retrieving end date parameter from request
    print(start, end)  # Printing start and end date for debugging

    if start and end:  # Checking if start and end date parameters are provided
        # Executing SQL query to get global summary of covid statistics within specified date range
        c.execute(
            f"""select sum(Confirmed), sum(deaths), sum(recovered) 
            from covid_data where DATE(last_update) <= '{end}' and DATE(last_update) >= {start};""")
    else:  # If start and end date parameters are not provided
        # Executing SQL query to get global summary of all covid statistics
        c.execute(
            f"""select sum(Confirmed), sum(deaths), sum(recovered) 
            from covid_data;""")
    content = c.fetchall()  # Fetching the query result
    c.close()  # Closing the cursor
    # Constructing response data
    data = [{'confirmed': confirmed, 'deaths': deaths, 'recovered': recovered} \
            for confirmed, deaths, recovered in content]
    return {
        'request_data': data  # Returning the response data
    }


#@app.route('/data')  # Route for retrieving data based on user input
#def data():
    #"""Retrieve data based on user input"""
    # Here we want to get the value of user (i.e. ?user=some-value)
    # user = request.args.get('user')  # Retrieving user input parameter from request


#labels = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered']  # List of labels

app.run()  # Running the Flask application


