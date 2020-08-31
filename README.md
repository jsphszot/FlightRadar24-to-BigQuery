# FlightRadar24-to-BigQuery

Scripts set up to scrape info from flightradar24 and upload to a BigQuery project. Later scheduled using Task Scheduler and run on a local server.

Goal of project was to track other airlines' movements each day and week. Sends bi-daily report by email with flight info by plane week and day of week (processfile/table.csv) as well as a printable png adequate for management (processfile/ResumenReporte.png). 

Keeps tracks of errors to process, writing to processfile/errorlogfile.txt

FlightRadar-to-BigQuery.py coordinates action stream and makes calls to the following files:
fr24wsFunctions.py - functions are set up for modular use.
credentials.json - user auth to GCP necessary for interacting with BigQuery.
keys.py - different key-value pairs, used to connect to gmail for automated report sending, slack for keeping live track of process, and flight radar api token.

processfile/ResumenReporte.png is built in MS Excel (processfile/FormatoReporte.xlsx) by above mentioned python code that updates cells, adds styling, saves a png of a dynamically sized area {changes daily depending on new info), and sent by email also automated using python.
