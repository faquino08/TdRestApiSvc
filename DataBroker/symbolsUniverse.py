from DataBroker.Sources.SymbolsUniverse.symbols import SymbolsUniverse
from DataBroker.Sources.SymbolsUniverse.holidayCalendar import getHolidaySchedule
import pytz
import logging
from datetime import datetime
import time
from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, DEBUG, APP_NAME

logger = logging.getLogger(__name__)

def symbolsUniverse(debug=False):
    '''
    Wrapper function to pull data from NASDAQ FTP Server and insert into Postgres Table.
    debug -> (boolean) Whether to log debug messages
    '''
    startTime = time.time() 
    logger.info(f'')
    logger.info(f'')
    logger.info(f'')
    logger.info(f'Symbols Universe')
    logger.info(f'Starting Run at: {startTime}')
    holidays = getHolidaySchedule()
    nyt = pytz.timezone('America/New_York')
    localDate = pytz.utc.localize(datetime.utcnow(), is_dst=None).astimezone(nyt).date()
    if localDate not in holidays:
        sym = SymbolsUniverse(postgresParams={
                "host": f'{POSTGRES_LOCATION}',
                "port": f'{POSTGRES_PORT}',
                "database": f'{POSTGRES_DB}',
                "user": f'{POSTGRES_USER}',
                "password": f'{POSTGRES_PASSWORD}',
                "application_name": f'{APP_NAME}SymbolsUni'
            },debug=debug,startTime=startTime)       
        sym.getListedSymbols()
    else:
        sym.log.info(str(localDate) + ' is a Holiday')
    sym.exit()