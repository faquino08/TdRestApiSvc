import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime

log = logging.getLogger(__name__)

def getHolidaySchedule():
    '''
    Retreive US stock market holiday schedule from nasdaq and return list of closed dates.
    '''
    log.info(f'')
    log.info(f'')
    log.info(f'')
    log.info(f'Getting Holiday Schedule')
    url = 'http://ftp.nasdaqtrader.com/Trader.aspx?id=Calendar'
    r = requests.get(url)
    res = BeautifulSoup(r.content, 'html.parser')
    dates = res.select(".dataTable tbody:nth-child(2) tr td:nth-child(1)")
    notes = res.select(".dataTable tbody:nth-child(2) tr td:nth-child(3)")
    dates = [date.text for date in dates]
    result = []
    for date in dates:
        index = dates.index(date)
        if notes[index].text == "Closed":
            result.append(datetime.strptime(date, '%B %d, %Y').date())
    return result
