from DataBroker.main import Main
import pytz
from datetime import datetime
from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, DEBUG, APP_NAME

def equity_freq(debug=DEBUG):
    '''
    Wrapper function to calculate frequency of equities in indices.
    debug -> (boolean) Whether to log debug messages
    '''
    main = Main(
        postgresParams={
            "host": f'{POSTGRES_LOCATION}',
            "port": f'{POSTGRES_PORT}',
            "database": f'{POSTGRES_DB}',
            "user": f'{POSTGRES_USER}',
            "password": f'{POSTGRES_PASSWORD}',
            "application_name": f'{APP_NAME}EquityFreq'
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
        moversOnly=True
    )
    main.runEquityFreqTable()
    main.exit()