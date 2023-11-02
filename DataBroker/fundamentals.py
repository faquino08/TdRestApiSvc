from DataBroker.main import Main
from DataBroker.Sources.SymbolsUniverse.holidayCalendar import getHolidaySchedule
import pytz
from datetime import datetime
from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, APP_NAME

def fundamentals(debug=False,fullMarket=False):
    '''
    Wrapper function to request fundamental data from TD Ameritrade.
    debug -> (boolean) Whether to log debug messages
    '''
    holidays = getHolidaySchedule()
    nyt = pytz.timezone('America/New_York')
    localDate = pytz.utc.localize(datetime.utcnow(), is_dst=None).astimezone(nyt).date()
    main = Main(
        postgresParams={
            "host": f'{POSTGRES_LOCATION}',
            "port": f'{POSTGRES_PORT}',
            "database": f'{POSTGRES_DB}',
            "user": f'{POSTGRES_USER}',
            "password": f'{POSTGRES_PASSWORD}',
            "application_name": f'{APP_NAME}Fundamentals'
        },
        debug=debug,
        client_id='KOOWEZGOW4WT4S5RFRGCOOLGLZCUCPOA',
        tablesToInsert=['tdoptionsdata','tdpricehistory_min','tdpricehistory_daily','tdpricehistory_weekly','tdmoversdata','tdstockmktquotedata','tdfundamentaldata'],
        symbolTables={
            "Uni": "listedsymbols",
            "Dji_tdscan":"dji_tdscan",
            "Nasd100_tdscan":"nasd100_tdscan",
            "Optionable_tdscan":"optionable_tdscan",
            "PennyOptions_tdscan":"pennyincrementoptions_tdscan",
            "Russell_tdscan":"russell_tdscan",
            "Sp400_tdscan":"sp400_tdscan",
            "Sp500_tdscan":"sp500_tdscan",
            "WeeklyOptions_tdscan":"weeklyoptions_tdscan",
            "Movers":"tdmoversdata",
            "Sectors":"sectors_tdscan"
        },
        assetTypes={
            "Uni":"EQUITY",
            "Dji_tdscan":"EQUITY",
            "Nasd100_tdscan":"EQUITY",
            "Optionable_tdscan":"EQUITY",
            "PennyOptions_tdscan":"EQUITY",
            "Russell_tdscan":"EQUITY",
            "Sp400_tdscan":"EQUITY",
            "Sp500_tdscan":"EQUITY",
            "WeeklyOptions_tdscan":"EQUITY",
            "Movers":"EQUITY",
            "Sectors":"EQUITY"
        },
        moversOnly=False,makeFreqTable=False)
    if localDate not in holidays:
        main.runTdRequests(minute=False,daily=False,weekly=False,quote=False,fundamentals=True,options=False,fullMarket=fullMarket)
    else:
        main.log.info(str(localDate) + ' is a Holiday')
    main.exit()