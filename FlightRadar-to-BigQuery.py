# remaking FR scrape
# fr24ws
# https://docs.google.com/spreadsheets/d/1y-3K9e47GWZTt3iPAH4tgRfey2IgZCp2a1-FD5adSQ8/edit#gid=673511596

Testing = True

### Set Parameters
# region ------------------------------------------------------------

from fr24wsFunctions import Routing, GetGoogleSheetInfo, processTime, fr24ws, Clean2GBQload, queryGBQ2DF, formatPNG, SendEmails
import sys
from keys import SenderPwd


# If testing true, don't send error messages to error log file
# send errors to log file
# WhichProcess="NAM" # debugging
if Testing:
    WhichProcess="EUR"
else:
    sys.stderr = open('errorlogfile.txt', 'w')
    sys.stdout = open('consolelogfile.txt', 'w')
    WhichProcess=sys.argv[1]

# endregion
# -------------------------------------------------------------------

### Activate timer and time variables
# region ------------------------------------------------------------

timer=processTime()

intDay=timer.intDay
monthDay=timer.monthDay
hourMins=timer.hourMins
todayfrmtd=timer.todayfrmtd
scrapedateid=timer.scrapedateid
thisWeek=timer.thisWeek
pageTimeStamp=timer.NowTimeStamp

# timer.TotalRunningTime('showmethemoney')

# endregion
# -------------------------------------------------------------------

### fr24ws 
# includes GetRegNums(), LoopOverAlnCodes(), Flights2DataFrame(), Geo2DataFrame(), fr24wsSlackMsg
# region ------------------------------------------------------------

fr24=fr24ws(Testing=Testing)

### Activate Slack Message Poster
### Initiate Greeting to All
SlackPoster = fr24.fr24wsSlackMsg
SlackPoster.Hello(monthDay, hourMins)

### Activate GoogleSheets Getter
fr24wsGS=GetGoogleSheetInfo('fr24ws')
CompetenciaList=fr24wsGS.Airlines(WhichProcess, False)
AllAirlines=fr24wsGS.Airlines("Todas", True)
guachas=fr24wsGS.getWS("MatriculasGuachas", True)
GuachasRegNums=fr24wsGS.JoinWachas(guachas, AllAirlines)

Geographic=fr24wsGS.Geo()
ListaCorreos=fr24wsGS.Mails(Testing)

### Get all RegNums + Guachas
# AircraftRegNums=fr24ws.LoopOverAlnCodes(CompetenciaList) + GuachasRegNums # uncomment
AircraftRegNums=GuachasRegNums

# import re
# regexps=re.compile(r'atlas|polar|skylease|western|avianca|kalitta')
# wnatednames=[x for x in CompetenciaList if re.search(regexps, x["Airline"].lower())]
# AircraftRegNums=fr24ws.LoopOverAlnCodes(wnatednames)

### Loop over all RegNums get Hist Flight Info
flightsLoL, geoLoJ = fr24.GetFlightInfo(AircraftRegNums) # Testing

### Write scraped info to DFs
AircraftItinerario=fr24.Flights2DataFrame(flightsLoL)
GeoFrame=fr24.Geo2DataFrame(geoLoJ)

### Cross GeoFrame with Continent-country-city info, upload to GBQ and drop duplicates -> Base Paramétrica Geografía
### Cross Flight info with Paramétrica Geografía, upload to GBQ and then drop all duplicates
cleanDFs = Clean2GBQload(GeoFrame, Geographic, AircraftItinerario, Testing=Testing)
cleanDFs.Geo2GBQ()
cleanDFs.Flights2GBQ()

# endregion
# -------------------------------------------------------------------

### Query clean data
# region ------------------------------------------------------------

# set up querier object, then run it's modules
querier=queryGBQ2DF()
# FlightInfoDF=querier.getQuery(test=True)
FlightInfoDF=querier.getRecentFlightData()

# endregion
# -------------------------------------------------------------------

### Restructure Data: generate routings
# region ------------------------------------------------------------

Rtng = Routing()
OrgDesList=["NAM-SAM", "BOG-SAM", "EUR-NAM", "EUR-SAM", "ASIA-SAM", "ASIA-NAM", "ASIA-EUR", "OCE-SAM"]
sendtable=Rtng.Run(FlightInfoDF, OrgDesList=OrgDesList)

# endregion
# -------------------------------------------------------------------

# TODO -  Exception: Failed locating range 'Todos'!A1:G0
### synthesied .png format for managers
formatPNG(tableFrame=sendtable)

### send email if sendemail argv is True
SendEmails(ReceiverEmails=ListaCorreos, SenderEmail="equipo.planificacionrmcargo@gmail.com", SenderPwd=SenderPwd, todayfrmtd=todayfrmtd)

smsg="Routed data sent by email\nProcess closed"
SlackPoster.fr24wsSlackMsg.Post(text=smsg)
