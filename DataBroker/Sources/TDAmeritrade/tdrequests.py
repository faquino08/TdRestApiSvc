from symtable import Symbol
from pyparsing import Regex
import requests
import psycopg2
import psycopg2.extras
import pandas as pd
import pandas.io.sql as sqlio
import datetime
import time
import pytz
import math
from ratelimiter import RateLimiter
import logging
from types import SimpleNamespace
from  DataBroker.Sources.TDAmeritrade.database import databaseHandler
import json
import copy
from constants import DEBUG

class tdRequests:
    def __init__(self,postgresParams={},debug=False,client_id='',tablesToInsert=[],dbHandler=None):
        '''
        Class to pull data on US Stocks from TD Ameritrade REST API.
        postgresParams -> (dict) Dict with keys host, port, database, user, \
                            password for Postgres database
        debug   -> (boolean) Whether to record debug logs
        client_id   -> (str) TD Ameritrade REST API client ID
        tablesToInsert  -> (list) List of string with table names to insert
        symbolsCount    -> (int) Current count of successful requests to save\
                            for later
        '''
        self.symbolsCount = 0
        self.nyt = pytz.timezone('America/New_York')
        if len(client_id) > 0:
            self.client_id = client_id
            self.tableToInsert = tablesToInsert
            if DEBUG:
                logging.basicConfig(
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                    datefmt="%m-%d %H:%M:%S",
                    handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=self.nyt).date()}.txt'), logging.StreamHandler()],
                )
            else:
                logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                    datefmt="%m-%d %H:%M:%S",
                    handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=self.nyt).date()}.txt'), logging.StreamHandler()],
                )
            self.log = logging.getLogger(__name__)
            self.postgres = postgresParams

            # Connect to Postgres
            if dbHandler is None:
                self.db = databaseHandler(self.postgres)
            else:
                self.db = dbHandler
            self.startTime = time.time() 
            self.log.info(f'')
            self.log.info(f'')
            self.log.info(f'')
            self.log.info(f'TDAmeritrade Databroker')
            self.log.info(f'Starting Run at: {self.startTime}')

            self.payload = {'apikey':client_id}
            self.optionsPayload = {'apikey':client_id,
                'contractType':'ALL',
                'range':'ALL',
                'expMonth':'ALL',
                'optionType':'ALL'}
            
            # Important variables to initialize
            self.data = {}
            self.lastTime = {'minute':[],'daily':[],'weekly':[]}
            self.indices = ['$DJI','$COMPX','$SPX.X']
            for table in self.tableToInsert:
                self.data[table] = []
            self.rate_limiter = RateLimiter(max_calls=75,period=60,callback=self.execute_mogrify)

            # Columns for charvar or text
            self.stringCols = ["assetType","assetMainType","cusip","symbol","description","bidId","askId","lastId","exchange","exchangeName","securityStatus","divDate",'callExpDateMap','putExpDateMap',"status","strategy"]

            # Columns to exclude from Insert .ie repetitive of another column or empty col
            self.excludedCols = ["symbol","underlying"]

            # Query to get Service Hours for each Asset Class
            hoursSql = "SELECT  * FROM \"DATASERVICEHOURS\";"

            try:
                colNames = self.db.getColNamesFromSchema('DATASERVICEHOURS')
                self.db.cur.execute(hoursSql)
                self.hours = pd.DataFrame(self.db.cur.fetchall(),columns=colNames).set_index("td_service_name")
                self.db.conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
            #self.hours = sqlio.read_sql_query(hoursSql,self.db.connAlch,index_col="asset_type")
            
            self.r = 0
    
    def exit(self):
        '''
        Exit class. Log Runtime. And close database handler.
        '''
        i = 0
        for key in self.data:
            if len(self.data[key]) > 0 and i == 0:
                i += 1
                self.execute_mogrify(None)
        self.endTime = time.time() 
        self.log.info("Exiting Td Ameritrade")
        self.log.info(f'Ending Run at: {self.endTime}')
        self.log.info(f'Runtime: {self.endTime - self.startTime}')
        return self.symbolsCount

    def calcDayMinEndDate(self,starttime='',endtime='',currentDatetime=datetime.datetime.today()):
        """ 
        Calculate End Date for Daily/Minute frequency requests based on when in week script is run if middle of trading will end at yesterday's closing date. Returns Timestamp in Milliseconds.
        starttime -> (str) String for opening time for TD Rest Api
        endtime -> (str) String for closing time for TD Rest Api
        currentDatetime -> (datetime) Datetime object to compare starttime and \
                            endtime to
        """
        beforeCloseCheck = currentDatetime.timetz() < endtime
        afterCloseCheck = currentDatetime.timetz() >= endtime
        if (beforeCloseCheck):
            yesterday = currentDatetime - datetime.timedelta(days=1)
            timestamp = datetime.datetime.combine(\
                yesterday.date(), 
                endtime)
            timestamp = self.nyt.localize(timestamp).timestamp()
            timestamp = math.trunc(timestamp*1000)
        elif (afterCloseCheck):
            timestamp = datetime.datetime.combine(\
                currentDatetime.date(), 
                endtime)
            timestamp = self.nyt.localize(timestamp).timestamp()
            timestamp = math.trunc(timestamp*1000)
        else:
            return None
        return timestamp

    def findLastTimestamp(self,frequency):
        """ 
        Gets last timestamp in Postgres for given frequency. Returns Timestamp in Milliseconds.
        frequency -> (str) Either 'minute', 'daily', 'weekly'
        """
        # Query to get timestamp of latest candle recorded in db
        lastTimeSql = "SELECT \"symbol\",MAX(datetime) FROM \"{}\" GROUP BY \"symbol\" ORDER BY \"symbol\""
        if (frequency == 'minute'):
            try:
                self.db.cur.execute(lastTimeSql.format("tdpricehistory_min"))
                if self.db.cur.fetchall() is None:
                    self.db.conn.commit()
                    return None
                lastTimestamp = self.db.cur.fetchone()[0] + 43200000
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
                return None
        elif (frequency == 'daily'):
            try:
                self.db.cur.execute(lastTimeSql.format("tdpricehistory_daily"))
                if self.db.cur.fetchone()[0] is None:
                    self.db.conn.commit()
                    return None
                lastTimestamp = self.db.cur.fetchone()[0] + 86400000
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
                return None
        elif (frequency == 'weekly'):
            try:
                self.db.cur.execute(lastTimeSql.format("tdpricehistory_weekly"))
                if self.db.cur.fetchone()[0] is None:
                    self.db.conn.commit()
                    return None
                lastTimestamp = self.db.cur.fetchone()[0] + 604800000
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
                return None
        else:
            self.log.error("Invalid frequency")
            return None
        self.db.conn.commit()
        return lastTimestamp
    
    def findLastTimestampPerSymbol(self,frequency):
        self.log.info("findLastTimestampPerSymbol")
        # Query to get timestamp of latest candle recorded in db
        lastTimeSql = "SELECT \"symbol\", MAX(datetime) FROM \"{}\" GROUP BY \"symbol\" ORDER BY \"symbol\""
        if (frequency == 'minute'):
            try:
                self.db.cur.execute(lastTimeSql.format("tdpricehistory_min"))
                data = self.db.cur.fetchall()
                if len(data) == 0:
                    self.db.conn.commit()
                    return None
                lastTimestamp = pd.DataFrame(data,columns=["symbol","datetime"]).set_index('symbol')
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
                return None
        elif (frequency == 'daily'):
            try:
                self.db.cur.execute(lastTimeSql.format("tdpricehistory_daily"))
                data = self.db.cur.fetchall()
                if len(data) == 0:
                    self.db.conn.commit()
                    return None
                lastTimestamp = pd.DataFrame(data,columns=["symbol","datetime"]).set_index('symbol')
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
                return None
        elif (frequency == 'weekly'):
            try:
                self.db.cur.execute(lastTimeSql.format("tdpricehistory_weekly"))
                data = self.db.cur.fetchall()
                if len(data) == 0:
                    self.db.conn.commit()
                    return None
                lastTimestamp = pd.DataFrame(data,columns=["symbol","datetime"]).set_index('symbol')
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
                return None
        else:
            self.log.error("Invalid frequency")
            return None
        self.db.conn.commit()
        self.lastTime[frequency] = lastTimestamp
        return None

    def calcWeeklyEndDate(self,endtime,currentDatetime):
        """ 
            Calculate End Date for weekly requests based on when in week script is run. Returns Timestamp in Milliseconds.
            endtime -> (str) String for closing time for TD Rest Api
            currentDatetime -> (datetime) Datetime object to compare \
                                    starttime and endtime to
        """
        today = currentDatetime
            
        if today.weekday() <= 3 or today.weekday() > 4:
            # If today is not Friday then calculate last Monday's timestamp
            delta = 7 + today.weekday()
            lastMonday = today - datetime.timedelta(days=delta)
            lastMonday = datetime.datetime.combine(\
                lastMonday.date(),
                endtime,
                tzinfo=datetime.timezone(-datetime.timedelta(hours=5))).timestamp()
            lastMonday = math.trunc(lastMonday*1000)
            return lastMonday
        else:
            today = datetime.datetime.combine(\
                today.date(),
                endtime,
                tzinfo=datetime.timezone(-datetime.timedelta(hours=5))).timestamp()
            today = math.trunc(today*1000)
            return today

    def getMinuteHistory(self,symbol,req,starttime,endtime,currentDatetime,lastTime) -> list or None:
        '''
        Get minute price history for given symbol.
        symbol -> (str) Symbol to pull price history data for
        req -> (str) Td Ameritrade REST API Endpoint
        starttime -> (str) String for opening time for TD Rest Api
        endtime -> (str) String for closing time for TD Rest Api
        currentDatetime -> (datetime) Datetime object to compare starttime and \
                            endtime to
        lastTime -> (str) Timestamp in milliseconds of last entry in \
                            database
        '''
        # Define our payload
        dataList = []
        payload = {'apikey':self.client_id,
                    'periodType':'day',
                    'period':'10',
                    'frequency(Type)':'minute',
                    'frequency':'1',
                    'needExtendedHoursData':'true'}

        req = req.format(symbol)

        indexLabel = {'symbol':symbol}
        
        # Make Minutes Price History Requests
        if (lastTime is not None):
            if type(lastTime) == pd.DataFrame:
                if symbol in lastTime.index:
                    payload['startDate'] = lastTime.loc[symbol]["datetime"]
                    payload.pop('period')
            else:
                payload['startDate'] = lastTime
                payload.pop('period')
        payload['endDate'] = self.calcDayMinEndDate(starttime,endtime,\
                                                       currentDatetime)
        if 'startDate' in payload.keys():
            self.log.info("Updating Minute Ticker: %s Start: %s End: %s" % (symbol,payload['startDate'],payload['endDate']))
            self.log.debug(payload)
        else:
            self.log.info("New Minute Ticker: %s End: %s" % (symbol,payload['endDate']))
            self.log.debug(payload)
        try:
            content = requests.get(url = req, params = payload)
            try:
                data = content.json()
            except json.JSONDecodeError as error:
                self.log.error(str(error.msg))
                return None
            if (data['empty']==False):
                candles = data['candles']
                if type(lastTime) == pd.DataFrame:
                    if symbol in lastTime.index:
                        if int(candles[-1]["datetime"]) <= int(payload['startDate']):
                            self.log.info("No New Entries Minute Ticker: %s" % (symbol))
                            return None
                        else:
                            candles = [candle for candle in candles if int(candle['datetime']) > int(payload['startDate'])]
                dataList = [list(dict(indexLabel, **item).values()) for item in candles]
                return dataList
            else:
                return None
        except requests.ConnectionError as e:
            self.log.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            self.log.error(str(e))            
            return None
        except requests.Timeout as e:
            self.log.error("OOPS!! Timeout Error")
            self.log.error(str(e))
            return None
        except requests.RequestException as e:
            self.log.error("OOPS!! General Error")
            self.log.error(str(e))
            return None
        except KeyboardInterrupt:
            self.log.error("Someone closed the program")

    def getDailyHistory(self,symbol,req,starttime,endtime,currentDatetime,lastTime) -> list or None:
        '''
        Get daily price history for given symbol.
        symbol -> (str) Symbol to pull price history data for
        req -> (str) Td Ameritrade REST API Endpoint
        starttime -> (str) String for opening time for TD Rest Api
        endtime -> (str) String for closing time for TD Rest Api
        currentDatetime -> (datetime) Datetime object to compare starttime and \
                            endtime to
        lastTime -> (str) Timestamp in milliseconds of last entry in \
                            database
        '''
        # Define our payload
        payload = {'apikey':self.client_id,
                    'periodType':'month',
                    'period':'6',
                    'frequencyType':'daily',
                    'frequency':'1',
                    'needExtendedHoursData':'true'}

        req = req.format(symbol)

        indexLabel = {'symbol':symbol}

        # Make Daily Price History Requests
        if (lastTime is not None):
            if type(lastTime) == pd.DataFrame:
                if symbol in lastTime.index:
                    payload['startDate'] = lastTime.loc[symbol]["datetime"]
                    payload.pop('period')
            else:
                payload['startDate'] = lastTime
                payload.pop('period')
        payload['endDate'] = self.calcDayMinEndDate(starttime,endtime,\
                                                         currentDatetime)
        if 'startDate' in payload.keys():
            self.log.info("Updating Daily Ticker: %s Start: %s End: %s" % (symbol,payload['startDate'],payload['endDate']))
            self.log.debug(payload)
        else:
            self.log.info("New Daily Ticker: %s End: %s" % (symbol,payload['endDate']))
            self.log.debug(payload)
        try:    
            content = requests.get(url = req, params = payload)
            try:
                data = content.json()
            except json.JSONDecodeError as error:
                #self.log.info(f"{symbol} Daily History: Empty response")
                self.log.error(str(error.msg))
                return None
            if (data['empty']==False):
                candles = data['candles']
                if type(lastTime) == pd.DataFrame:
                    if symbol in lastTime.index:
                        if int(candles[-1]["datetime"]) <= int(payload['startDate']):
                            self.log.info("No New Entries Daily Ticker: %s" % (symbol))
                            return None
                        else:
                            candles = [candle for candle in candles if int(candle['datetime']) > int(payload['startDate'])]
                dataList = [list(dict(indexLabel, **item).values()) for item in candles]
                return dataList
            else:
                return None
        except requests.ConnectionError as e:
            self.log.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            self.log.error(str(e))            
            return
        except requests.Timeout as e:
            self.log.error("OOPS!! Timeout Error")
            self.log.error(str(e))
            return
        except requests.RequestException as e:
            self.log.error("OOPS!! General Error")
            self.log.error(str(e))
            return
        except KeyboardInterrupt:
            self.log.error("Someone closed the program")

    def getWeeklyHistory(self,symbol,req,endtime,currentDatetime,lastTime) -> list or None:
        '''
        Get weekly price history for given symbol.
        symbol -> (str) Symbol to pull price history data for
        req -> (str) Td Ameritrade REST API Endpoint
        endtime -> (str) String for closing time for TD Rest Api
        currentDatetime -> (datetime) Datetime object to compare starttime and \
                            endtime to
        lastTime -> (str) Timestamp in milliseconds of last entry in \
                            database
        '''
        # Define our payload
        payload = {'apikey':self.client_id,
                    'periodType':'year',
                    'period':'20',
                    'frequencyType':'weekly',
                    'frequency':'1',
                    'needExtendedHoursData':'true'}
        
        req = req.format(symbol)

        indexLabel = {'symbol':symbol}

        # Make Weekly Price History Requests
        if (lastTime is not None):
            if type(lastTime) == pd.DataFrame:
                if symbol in lastTime.index:  
                    payload['startDate'] = lastTime.loc[symbol]["datetime"]
                    payload.pop('period')
            else:
                payload['startDate'] = lastTime
                payload.pop('period')
        payload['endDate'] = self.calcWeeklyEndDate(endtime,currentDatetime)
        if 'startDate' in payload.keys():
            self.log.info("Updating Weekly Ticker: %s Start: %s End: %s" % (symbol,payload['startDate'],payload['endDate']))
            self.log.debug(payload)
        else:
            self.log.info("New Weekly Ticker: %s End: %s" % (symbol,payload['endDate']))
            self.log.debug(payload)
        try:
            content = requests.get(url = req, params = payload)
            try:
                data = content.json()
            except json.JSONDecodeError as error:
                #self.log.info(f"{symbol} Weekly History: Empty response")
                self.log.error(str(error.msg))
                return None
            if (data['empty']==False):
                candles = data['candles']
                if type(lastTime) == pd.DataFrame:
                    if symbol in lastTime.index:
                        if int(candles[-1]["datetime"]) <= int(payload['startDate']):
                            self.log.info("No New Entries Weekly Ticker: %s" % (symbol))
                            return None
                        else:
                            candles = [candle for candle in candles if int(candle['datetime']) > int(payload['startDate'])]
                dataList = [list(dict(indexLabel, **item).values()) for item in candles]
                return dataList
            else:
                return None
        except requests.ConnectionError as e:
            self.log.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            self.log.error(str(e))            
            return
        except requests.Timeout as e:
            self.log.error("OOPS!! Timeout Error")
            self.log.error(str(e))
            return
        except requests.RequestException as e:
            self.log.error("OOPS!! General Error")
            self.log.error(str(e))
            return
        except KeyboardInterrupt:
            self.log.error("Someone closed the program")

    def requestPriceHistory(self,symbol,frequency):
        '''
        Wrapper Function for requesting price history from TD Ameritrade Rest API.
        symbol -> (str) Symbol to pull price history data
        frequency -> (str) Either 'minute', 'daily', 'weekly'
        '''
        # Define our endpoint
        priceHistEndpoint = \
            r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory"
        td_service_name = symbol.td_service_name
        start = self.hours.loc[td_service_name].td_start
        end = self.hours.loc[td_service_name].td_end
        today = datetime.datetime.now().astimezone(self.nyt)
        if (frequency == 'minute'):
            res = self.getMinuteHistory(symbol.name,priceHistEndpoint,start,end,today,self.lastTime[frequency])
            if(res is not None):
                self.symbolsCount += 1
                self.data['tdpricehistory_min'].extend(res)
                return
            else:
                return
        elif (frequency == 'daily'):
            res = self.getDailyHistory(symbol.name,priceHistEndpoint,start,end,today,self.lastTime[frequency])
            if(res is not None):
                self.symbolsCount += 1
                self.data['tdpricehistory_daily'].extend(res)
                return
        elif (frequency == 'weekly'):
            res =  self.getWeeklyHistory(symbol.name,priceHistEndpoint,end,today,self.lastTime[frequency])
            if(res is not None):
                self.symbolsCount += 1
                self.data['tdpricehistory_weekly'].extend(res)
                return
        else:
            self.log.error("Invalid frequency")
            return

    def requestFundamentals(self,symbol):
        '''
        Function for requesting fundamental data from TD Ameritrade Rest API.
        symbol -> (str) Symbol to pull price history data
        '''
        # Define our endpoint
        base_url = '''https://api.tdameritrade.com/v1/instruments?&symbol={stock_ticker}&projection={projection}'''
        # Define our payload
        payload = self.payload
        endpoint = base_url.format(stock_ticker = symbol,
        projection = 'fundamental')
        try:
            content = requests.get(endpoint, payload)
            if content.status_code == 200:
                try:
                    data = content.json()
                except json.JSONDecodeError:
                    self.log.info(f"Fundamental: {symbol} => Empty response")
                    data = {}
                self.r += 1
                if len(data) == 0 and self.r <= 1:
                    d = {"name": symbol}
                    sym = SimpleNamespace(**d)
                    self.makeRequest(sym,'fundamentals')
                    return
                elif len(data) == 0 and self.r > 1:
                    self.log.info("Fundamentals: " + symbol + "==> empty")
                    return
                elif 'error' in data.keys():
                    if 'restriction reached' in data['error']:
                        d = {"name": symbol}
                        sym = SimpleNamespace(**d)
                        self.log.error(symbol + ': Restriction reached')
                        self.makeRequest(sym,'fundamentals')
                        return
                else:
                    # Data will be a dict with the symbol name as a level one key and 'fundamental' as a level two key to access the returned values from the request
                    sym = symbol.upper()
                    data = data[sym]['fundamental']
                    dataList = list(data.values())
                    self.symbolsCount += 1
                    return self.data['tdfundamentaldata'].append(dataList)
            elif content.status_code == 401 or content.status_code == 403 or content.status_code == 404:
                self.log.info("Error Code: " + str(content.status_code) + " => " + symbol)
        except requests.ConnectionError as e:
            self.log.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            self.log.error(str(e))            
            return
        except requests.Timeout as e:
            self.log.error("OOPS!! Timeout Error")
            self.log.error(str(e))
            return
        except requests.RequestException as e:
            self.log.error("OOPS!! General Error")
            self.log.error(str(e))
            return
        except KeyboardInterrupt:
            self.log.error("Someone closed the program")

    def requestQuote(self,symbol):
        '''
        Function for requesting quote from TD Ameritrade Rest API.
        symbol -> (str) Symbol to pull price history data
        '''
        # Define our endpoint
        endpoint = r'https://api.tdameritrade.com/v1/marketdata/{}/quotes'.format(symbol)

        # Define our payload
        payload = self.payload
        # Intialize Dict that will be used for result
        data = {'symbol':symbol}
        # Make a request
        try:
            content = requests.get(url = endpoint, params = payload)
            try:
                if content.status_code == 200:
                    try:
                        data = content.json()
                    except json.JSONDecodeError:
                        self.log.info(f"Quote: {symbol} => Empty response")
                        data = {}
                    self.r += 1
                    if len(data) == 0 and self.r <= 1:
                        d = {"name": symbol}
                        sym = SimpleNamespace(**d)
                        self.makeRequest(sym,'quote')
                        return
                    elif len(data) == 0 and self.r > 1:
                        self.log.info("Quote: " + symbol + "==> empty")
                        return
                    elif 'error' in data.keys():
                        if 'restriction reached' in data['error']:
                            d = {"name": symbol}
                            sym = SimpleNamespace(**d)
                            self.log.error(symbol + ': Restriction reached')
                            self.makeRequest(sym,'quote')
                            return
                    else:
                        # Data will be a dict with the symbol name as the key to access the returned values from the request
                        sym = symbol.upper()
                        data = data[sym]
                        data = {k: "Null" if type(v) is str and v == ' ' or v == "" else v for k, v in data.items()}
                        data['description'] = data['description'].replace("'","''")
                        if 'assetSubType' in data.keys():
                            data.pop('assetSubType',None)
                        dataList = list(data.values())
                        self.symbolsCount += 1
                        return self.data['tdstockmktquotedata'].append(dataList)
                elif content.status_code == 401 or content.status_code == 403 or content.status_code == 404:
                    self.log.error("Error Code: " + str(content.status_code) + " => " + symbol)
            except ValueError:
                self.log.error('No Data: ' + symbol)       
        except requests.ConnectionError as e:
            self.log.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            self.log.error(str(e))            
            return
        except requests.Timeout as e:
            self.log.error("OOPS!! Timeout Error")
            self.log.error(str(e))
            return
        except requests.RequestException as e:
            self.log.error("OOPS!! General Error")
            self.log.error(str(e))
            return
        except KeyboardInterrupt:
            self.log.error("Someone closed the program")

    def requestOptionChain(self,symbol,strategy='SINGLE',strategyParams={},\
        excluded=["underlying"]):
        """
        Function for requesting options chain from TD Ameritrade Rest API.
        symbol -> (str) Symbol to pull price history data
        strategy -> (str) Strategy for options chain request.
        strategyParams -> (dict) Dict of custom strategy parameters in request
        """
        # Define our endpoint
        endpoint = 'https://api.tdameritrade.com/v1/marketdata/chains?&symbol={}'.format(symbol)
        # Define our payload
        basicPayload = self.optionsPayload
        basicPayload['strategy'] = strategy
        optionPayload = basicPayload | strategyParams
        # Intialize Dict that will be used for result
        data = {'symbol':symbol}
        # Make a request
        try:
            content = requests.get(url=endpoint,params=optionPayload)
            # Number of requests made
            if content.status_code == 200:
                try:
                    optionsData = content.json()
                except json.JSONDecodeError:
                    self.log.info(f"Options Chain: {symbol} => Empty response")
                    optionsData = {}
                self.r += 1
                if len(optionsData) == 0 and self.r <= 1:
                    d = {"name": symbol}
                    sym = SimpleNamespace(**d)
                    self.makeRequest(sym,'options')
                    return
                elif 'status' not in optionsData.keys() and self.r <= 1:
                    d = {"name": symbol}
                    sym = SimpleNamespace(**d)
                    self.makeRequest(sym,'options')
                    return
                elif optionsData['status'] == 'FAILED' and self.r <= 1:
                    d = {"name": symbol}
                    sym = SimpleNamespace(**d)
                    self.makeRequest(sym,'options')
                    return
                elif 'error' in data.keys():
                    if 'restriction reached' in data['error']:
                        d = {"name": symbol}
                        sym = SimpleNamespace(**d)
                        self.log.error(symbol + ': Option Chain Restriction reached')
                        self.makeRequest(sym,'options')
                        return
            
                if len(optionsData) == 0 and self.r > 1:
                    self.log.info("Options Chain: " + symbol + "==> empty")
                    return
                elif 'status' not in optionsData.keys() and self.r > 1:
                    self.log.info("Options Chain: " + symbol + "==> empty")
                    return
                elif optionsData['status'] == 'FAILED' and self.r > 1:
                    self.log.info("Options Chain: " + symbol + "==> FAILED")
                    return
                elif 'error' in data.keys():
                    if 'restriction reached' in data['error']:
                        self.log.error(symbol + ': Option Chain Restriction reached')
                        return
                else:
                    data.update(optionsData)
                for excludedCol in excluded:
                    if excludedCol in data:
                        data.pop(excludedCol)

                data = {k: str(v).replace("'",'"') if k in self.stringCols else v for k, v in data.items()}
                optionsToInsert = []
                for i in ['callExpDateMap','putExpDateMap']:
                    data[i] = data[i].replace('None','"Null"')
                    data[i] = data[i].replace("False", '"False"')
                    data[i] = data[i].replace("True", '"True"')
                    optionToInsert = {
                        "symbol": data["symbol"],
                        "underlyingsymbol": data["symbol"],
                        "status": data["status"],
                        "strategy": data["strategy"],
                        "interval": data["interval"],
                        "isDelayed": data["isDelayed"],
                        "isIndex": data["isIndex"],
                        "interestRate": data["interestRate"],
                        "underlyingPrice": data["underlyingPrice"],
                        "volatility": data["volatility"],
                        "underlyingVolatility": data["volatility"],
                        "daysToExpiration": data["daysToExpiration"],
                        "numberOfContracts": data["numberOfContracts"],
                    }
                    if len(data[i]) > 0:
                        chain = json.loads(data[i])
                        for expDate in chain.keys():
                            strikePrices = chain[expDate].keys()
                            for price in strikePrices:
                                option = chain[expDate][price][0]
                                #option = json.dump(option)
                                optionToInsert['expDate'] = expDate[:10]
                                optionToInsert['strikePrice'] = price
                                optionToInsert['type'] = i[:i.rindex('E')]
                                if type(option) == dict:
                                    for key in option.keys():
                                        optionToInsert[key] = option[key]
                                        if type(option[key]) == dict or type(option[key]) == list:
                                            s = ''.join(str(x) for x in option[key])
                                            optionToInsert[key] = s
                                #optionToInsert['option'] = option
                                #self.log.info(str(optionToInsert))
                                dataList = list(optionToInsert.values())
                                optionsToInsert.append(dataList)
                    else:
                        optionToInsert['expDate'] = "Null"
                        optionToInsert['strikePrice'] = "Null"
                        optionToInsert['type'] = "Null"
                        optionToInsert['option'] = "Null"
                        dataList = list(optionToInsert.values())
                        optionsToInsert.append(dataList)
                if len(optionsToInsert) >= 1:
                    self.symbolsCount += 1
                    for row in optionsToInsert:
                        self.data['tdoptionsdata'].append(row)
                    return
                else:
                    return
            elif content.status_code == 401 or content.status_code == 403 or content.status_code == 404:
                self.log.info("Error Code: " + str(content.status_code) + " => " + symbol)
        except requests.ConnectionError as e:
            self.log.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
            self.log.error(str(e))            
            return
        except requests.Timeout as e:
            self.log.error("OOPS!! Timeout Error")
            self.log.error(str(e))
            return
        except requests.RequestException as e:
            self.log.error("OOPS!! General Error")
            self.log.error(str(e))
            return
        except KeyboardInterrupt:
            self.log.error("Someone closed the program")

    def formMoversList(self,movers):
        '''
        Create combined list of movers in each index based on TD Ameritrade API.
        movers -> (dict) Dict of movers with the keys being the indices
        '''
        res = []
        for key in movers:
            arr = movers[key]
            for mover in arr:
                mover['indices'] = key
                res.append(mover)
        res = [list(mover.values()) for mover in res]
        return res

    def requestMovers(self,moverOptions={}):
        '''
        Function for requesting movers from TD Ameritrade Rest API.
        moverOptions -> (dict) Dict with key 'change' and either 'value' \
                            or 'percent' for request to API
        '''
        # Define our payload
        payload = self.payload
        moverPayload = payload | moverOptions
        movers = {}
        for index in self.indices:
            # Define our endpoint
            endpoint = 'https://api.tdameritrade.com/v1/marketdata/{}/movers'\
                .format(index)
            try:
                content = requests.get(url=endpoint,params=moverPayload)
                if content.status_code == 200:
                    try:
                        data = content.json()
                    except json.JSONDecodeError:
                        self.log.info(f"Movers: Empty response")
                        data = {}
                    self.r += 1
                    if len(data) == 0 and self.r <= 1:
                        self.makeRequest('retry','movers',moverOption=moverOptions)
                        return
                    elif len(data) == 0 and self.r > 1:
                        self.log.info("Movers: " + index + "==> empty")
                        return
                    else:    
                        data = [{k: "Null" if type(v) is str and v == ' ' or v == "" else v for k, v in mover.items()} for mover in data]
                        data = [{k: str(v).replace("'",'"') if k in self.stringCols else v for k, v in mover.items()} for mover in data]
                        self.symbolsCount += 1
                        movers[index] = data
                elif content.status_code == 401 or content.status_code == 403 or content.status_code == 404:
                    self.log.error("Error Code: " + str(content.status_code) + " => Movers")
            except requests.ConnectionError as e:
                self.log.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
                self.log.error(str(e))            
                return
            except requests.Timeout as e:
                self.log.error("OOPS!! Timeout Error")
                self.log.error(str(e))
                return
            except requests.RequestException as e:
                self.log.error("OOPS!! General Error")
                self.log.error(str(e))
                return
            except KeyboardInterrupt:
                self.log.error("Someone closed the program")
        moversList = self.formMoversList(movers)
        self.data['tdmoversdata'] = self.data['tdmoversdata'] + moversList
        return self.data['tdmoversdata']

    def execute_mogrify(self,until):
        '''
        Wrapper function for execute_mogrify to make it a callback function for rate_limiter.
        '''
        data = {}
        data = copy.deepcopy(self.data)
        for key in self.data:
            self.data[key].clear()
            self.log.info("mogrify: " + key)
            self.log.info('Total Length: ' + str(len(data[key])))
            if len(data[key]) > 0:
                mogrifyRes = self.db.execute_mogrify(data[key],key,insertTables=self.tableToInsert)
            else:
                mogrifyRes = 1
            if mogrifyRes == 0:
                self.data[key].clear()
        return

    def makeRequest(self,symbol,api,frequency=None,moverOption={'change':'percent'}):
        '''
        Wrapper function to route all API requests through the rate limiter.
        symbol -> (str) Symbol for API request
        api -> (str) Api to call either 'price', 'quote', \
                            'fundamentals', 'movers', 'options'
        frequency ->
        moverOption ->
        '''
        with self.rate_limiter:
            if (api == 'price'):
                if(bool(frequency)):
                    return self.requestPriceHistory(symbol,frequency)
                else:
                    self.log.error("Frequency not supplied to makeRequest()")
                    return
            elif (api == 'quote'):
                return self.requestQuote(symbol.name)
            elif (api == 'fundamentals'):
                return self.requestFundamentals(symbol.name)
            elif (api == 'movers'):
                if(bool(self.indices)):
                    return self.requestMovers(moverOption)
                else:
                    self.log.error("Indices not supplied to makeRequest()")
                    return
            elif (api == 'options'):
                return self.requestOptionChain(symbol.name)
            else:
                self.log.error("Invalid api specified in makeRequest()")
                return

    def resetAttemptCount(self):
        '''
        Helper function for resetting attempt count of API Requests.
        '''
        self.r = 0
        return
            