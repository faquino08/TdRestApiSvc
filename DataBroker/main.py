from asyncio.log import logger
import logging
import inspect
import os.path
import datetime
import time
import pytz
import psycopg2
import psycopg2.extras
from pprint import pprint
import pandas as pd
import pandas.io.sql as sqlio
from DataBroker.Sources.TDAmeritrade.tdrequests import tdRequests as td
from types import SimpleNamespace
from sqlalchemy import create_engine
from constants import DEBUG, APP_NAME
from  DataBroker.Sources.TDAmeritrade.database import databaseHandler

class Main:
    def __init__(self,postgresParams={},debug=False,client_id='',tablesToInsert=[],symbolTables={},assetTypes={},moversOnly=False,makeFreqTable=True):
        '''
        Main class for running different workflows for TD Ameritrade API requesta and getting universe of symbols.
        postgresParams -> (dict) Dict with keys host, port, database, user, \
                            password for Postgres database
        debug -> (boolean) Whether to record debug logs
        client_id -> (str) TD Ameritrade REST API client ID
        tablesToInsert -> (list) List of string with table names to insert
        symbolTables -> (dict) Dict where values are name of tables to \
                            update and one key must be 'Uni' for universe of \
                            symbols
        assetTypes -> (dict) Dict where values are asset types for each \
                            key in symbolTables and keys must be the same for \
                            both
        moversOnly -> (boolean) Whether to only run movers request
        '''
        self.movers = moversOnly
        nyt = pytz.timezone('America/New_York')
        self.today = datetime.datetime.now(tz=nyt).date()
        self.data = {}
        if DEBUG:
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M:%S",
                handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=nyt).date()}.txt'), logging.StreamHandler()],
            )
        else:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M:%S",
                handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=nyt).date()}.txt'), logging.StreamHandler()],
            )
        self.log = logging.getLogger(__name__)
        self.params = postgresParams
        self.symbolTables = symbolTables
        self.assetTypes = assetTypes
        self.startTime = time.time() 
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'Main')
        self.log.info(f'Starting Run at: {self.startTime}')
        self.connect()
        
        caller = inspect.stack()[1][3].upper()
        # Create New Run in RunHistory
        self.db.cur.execute('''
            INSERT INTO PUBLIC.financedb_RUNHISTORY ("Process","Startime") VALUES ('%s','%s') RETURNING "Id";
        ''' % (caller,self.startTime))
        self.runId = self.db.cur.fetchone()[0]
        self.db.conn.commit()
        self.td = td(self.params,debug,client_id,tablesToInsert=tablesToInsert,dbHandler=self.db)
        
        '''lastDate = self.td.db.getLastDate('tdequityfrequencytable','added')
        if lastDate != self.today or lastDate == None:
            if symbolTables.keys() == assetTypes.keys() and not self.movers and makeFreqTable:
                self.queueSecurities(symbolTables,assetTypes)
                self.makeTdFrequencyTable()
            elif self.movers:
                return self.log.info("Movers Only Run")
            else:
                return self.log.error("Non Matching Keys in symbolTables & assetTypes")'''

        if self.movers:
            return self.log.info("Movers Only Run")
        else:
            return self.log.error("Non Matching Keys in symbolTables & assetTypes")

    def exit(self):
        '''
        Exit class. Log Runtime. And shutdown logging.
        '''
        inserted = self.td.exit()
        self.endTime = time.time()
        # Update RunHistory With EndTime
        if not self.lastSymbol:
            self.lastSymbol = 'Null'
        self.db.cur.execute('''
            UPDATE PUBLIC.financedb_RUNHISTORY
            SET "Endtime"=%s,
                "SymbolsInsert"=%s,
                "LastSymbol"=%s
            WHERE "Id"=%s
        ''' % (self.endTime,inserted,self.lastSymbol,self.runId))
        self.db.conn.commit()
        self.db.exit()
        #self.connAlch.close()
        #self.log.info('Db Exit Status:')
        #self.log.info('SqlAlchemy:')
        #self.log.info(self.connAlch.closed)
        self.log.info("Closing Main")
        self.log.info(f'Ending Run at: {self.endTime}')
        self.log.info(f'Runtime: {self.endTime - self.startTime}')
        logging.shutdown()

    def runTdRequests(self,minute=False,daily=False,weekly=False,quote=False,fundamentals=False,options=False,fullMarket=False):
        '''
        Function for running TD API requests 
        minute -> (boolean) Whether to run minute price history flow
        daily -> (boolean) Whether to run daily price history flow
        weekly -> (boolean) Whether to run weekly price history flow
        quote -> (boolean) Whether to run quote flow
        fundamentals -> (boolean) Whether to run fundamentals flow
        options -> (boolean) Whether to run options flow
        '''
        priceHist = False
        if minute or daily or weekly:
            priceHist = True
        if not self.movers:
            if fundamentals:
                self.log.info('PriceHist: {}, Fundamentals: {}, FullMarket: {}'.format(priceHist,fundamentals,fullMarket))
                self.getTotalFreqTable(broker='Td',priceHist=priceHist,fundamentals=fundamentals,fullMarket=fullMarket)
                tdSymbols = self.equityFreqTable
                tdSymbols['td_service_name'] = 'QUOTE'
            else:
                if not fullMarket:
                    self.getTotalFreqTable(broker='Td',priceHist=priceHist)
                    tdSymbols = self.optionableFreqTable
                    tdSymbols['td_service_name'] = 'QUOTE'
                else:
                    self.getTotalFreqTable(broker='Td')
                    tdSymbols = self.equityFreqTable
                    tdSymbols['td_service_name'] = 'QUOTE'
            self.totalLen = len(tdSymbols)
        else:
            self.totalLen = 3
            self.td.resetAttemptCount()
            # Update RunHistory With Length Of Queue
            self.log.info('Updating runhistory Queue')
            self.db.cur.execute('''
                UPDATE PUBLIC.financedb_RUNHISTORY
                SET "SymbolsToFetch"=1
                WHERE "Id"=%s
            ''' % self.runId)
            self.db.conn.commit()
            return self.td.makeRequest(None,api='movers')
        try:
            if minute:
                self.td.findLastTimestampPerSymbol('minute')
                self.log.info("lastTime['minute']: " + str(self.td.lastTime['minute']))
            if daily:
                self.td.findLastTimestampPerSymbol('daily')
                self.log.info("lastTime['daily']: " + str(self.td.lastTime['daily']))
            if weekly:
                self.td.findLastTimestampPerSymbol('weekly')
                self.log.info("lastTime['weekly']: " + str(self.td.lastTime['weekly']))
        except (Exception) as error:
            self.log.error(error)
        # Update RunHistory With Length Of Queue
        self.log.info('Updating runhistory Queue')
        self.db.cur.execute('''
            UPDATE PUBLIC.financedb_RUNHISTORY
            SET "SymbolsToFetch"=%s
            WHERE "Id"=%s
        ''' % (len(tdSymbols),self.runId))
        self.db.conn.commit()
        for row in tdSymbols.itertuples():
            index = row.Index
            d = {"name": index, "td_service_name": row.td_service_name}
            sym = SimpleNamespace(**d)   
            if minute:
                self.td.resetAttemptCount()
                self.td.makeRequest(sym,'price','minute')
            if daily:
                self.td.resetAttemptCount()
                self.td.makeRequest(sym,'price','daily')
            if weekly:
                self.td.resetAttemptCount()
                self.td.makeRequest(sym,'price','weekly')
            if quote:
                self.td.resetAttemptCount()
                self.td.makeRequest(sym,'quote')
            if fundamentals:
                self.td.resetAttemptCount()
                self.td.makeRequest(sym,'fundamentals')
            if options:
                    if index in self.optionableFreqTable.index:
                        self.td.resetAttemptCount()
                        self.td.makeRequest(sym,'options')
            self.lastSymbol = index
        return

    def runEquityFreqTable(self):
        '''
        Get listed symbols and indices components and make tabulation of frequency.
        '''
        self.queueSecurities(self.symbolTables,self.assetTypes)
        self.makeTdFrequencyTable()
        return
                
    def queueSecurities(self,sqlTables={"Uni": "listedsymbols"},assetTables={"Uni":"EQUITY"}):
        '''
        Get securities and indices components from Postgres.
        sqlTables -> (dict) Dict where values are tables of symbols \
                            universe and indices. MUST HAVE 'Uni' KEY
        assetTables -> (dict) Dict where values are asset types for each \
                            key in sqlTables and keys must be the same for \
                            both
        '''
        self.symbols = {}
        self.log.info("Weekday: " + str(self.today.isoweekday()))
        if self.today.isoweekday() == 1:
            filterDay = "CURRENT_DATE - INTERVAL '3 DAYS'"
            moverFilterDay = "CURRENT_DATE - INTERVAL '4 DAYS'"
        else: 
            filterDay = "CURRENT_DATE"
            moverFilterDay = "CURRENT_DATE - INTERVAL '1 DAYS'"
        for key in sqlTables.keys():
            getSql = "SELECT * FROM \"%s\" WHERE DATE(\"Updated\" AT TIME ZONE 'US/Eastern') = %s ORDER BY \"Symbol\"" % (sqlTables[key], filterDay)
            if key != "Uni":
                getSql = "SELECT * FROM \"%s\" WHERE DATE(\"Updated\" AT TIME ZONE 'US/Eastern') >= %s ORDER BY \"Symbol\"" % (sqlTables[key], filterDay)
            if key == "Movers":
                getSql = "SELECT * FROM \"%s\" WHERE DATE(\"dateadded\" AT TIME ZONE 'US/Eastern') >= %s ORDER BY \"Symbol\"" % (sqlTables[key], moverFilterDay)

            try:
                colNames = self.db.getColNamesFromSchema(sqlTables[key])
                self.db.cur.execute(getSql)
                self.symbols[key] = pd.DataFrame(self.db.cur.fetchall(),columns=colNames).set_index("Symbol")
                self.db.conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                self.log.error("Error: %s" % error)
                self.db.conn.rollback()
            '''self.symbols[key] = sqlio.read_sql_query(getSql,self.connAlch,index_col="Symbol")'''

            self.symbols[key]["asset_type"] = assetTables[key]
            self.log.info(key)
            self.log.info(self.symbols[key].index)
            self.symbols[key].loc[self.symbols[key].index.str[0] == '/',"asset_type"] = "FUTURE"
        return

    def makeTdFrequencyTable(self,excludeUni=True):
        '''
        Create tabulation of frequencies in indices.
        excludeUni -> (boolean) Whether to count the symbols universe in the \
                        the tabulation
        '''
        missingSymbols ={}
        symUni = self.symbols['Uni']
        tdMainCount = symUni["SymbolTd"].value_counts()
        if excludeUni:
            tdMainCount = tdMainCount.replace(1,0)
        symUni['Symbol'] = symUni.index.str[:]
        freqTable = symUni[["SymbolTd","Symbol","SymbolIb","asset_type","Security Name"]]
        #pd.DataFrame({"SymbolTd": symUni["SymbolTd"].array, "Symbol": symUni.index.array, "SymbolIb": symUni["SymbolIb"].array, "asset_type": symUni["asset_type"].array, "Security Name": symUni["Security Name"].array})
        freqTable['Sector'] = ''
        freqTable['Industry'] = ''
        freqTable['Sub-Industry'] = ''
        freqTable = freqTable.set_index('SymbolTd')
        for key in self.symbols.keys():
            if ('tdscan' in key):
                sym = self.symbols[key].loc[self.symbols[key]['asset_type'].str[0:] == 'EQUITY']
                count = sym.index.value_counts()
                tdMainCount = tdMainCount.add(count,axis=0,fill_value=0)
                countIndices = count.index.str[:]
                '''if len(freqTable) > 0:
                    mainTableIndices = freqTable.index.str[:].values
                else:
                    mainTableIndices = []'''
                colName = key[:-7]
                freqTable[colName] = 0

                # Add new symbols to mainTable
                newSymbols = tdMainCount.index.difference(freqTable.index)
                for index in list(newSymbols):
                    newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                    freqTable = pd.concat([freqTable,newRow])
                    missingSymbols[index] = key
                '''for index, item in tdMainCount.iteritems():
                    if index not in mainTableIndices:
                        newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                        freqTable = pd.concat([freqTable,newRow])
                        missingSymbols[index] = key'''
                # Increment Count in Main Table
                freqTable.loc[freqTable.index.isin(list(countIndices)),colName] = 1
                '''for index, row in freqTable.iterrows():
                    if index in countIndices:
                        freqTable.loc[index,colName] = 1'''
            elif key == "Movers":
                sym = self.symbols[key].loc[self.symbols[key]['asset_type'].str[0:] == 'EQUITY']
                count = sym.index.value_counts()
                tdMainCount = tdMainCount.add(count,axis=0,fill_value=0)
                countIndices = count.index.str[:]
                #mainTableIndices = freqTable.index.str[:].values
                colName = key
                freqTable[colName] = 0

                # Add new symbols to mainTable
                newSymbols = tdMainCount.index.difference(freqTable.index)
                for index in list(newSymbols):
                    newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                    freqTable = pd.concat([freqTable,newRow])
                    missingSymbols[index] = key
                '''for index, item in tdMainCount.iteritems():
                    if index not in mainTableIndices:
                        newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                        freqTable = pd.concat([freqTable,newRow])
                        missingSymbols[index] = key'''
                # Increment Count in Main Table
                freqTable.loc[freqTable.index.isin(list(countIndices)),colName] = 1
                '''for index, row in freqTable.iterrows():
                    if index in countIndices:
                        freqTable.loc[index,colName] = 1'''
            elif key == "Sectors":
                sym = self.symbols[key].loc[self.symbols[key]['asset_type'].str[0:] == 'EQUITY']
                count = sym.index.value_counts()
                tdMainCount = tdMainCount.add(count,axis=0,fill_value=0)    
                countIndices = count.index.str[:]
                #mainTableIndices = freqTable.index.str[:].values
                colName = key
                freqTable[colName] = 0

                # Add new symbols to mainTable
                newSymbols = tdMainCount.index.difference(freqTable.index)
                for index in list(newSymbols):
                    newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                    freqTable = pd.concat([freqTable,newRow])
                    missingSymbols[index] = key
                '''for index, item in tdMainCount.iteritems():
                    if index not in mainTableIndices:
                        newRow = pd.DataFrame([[self.tdtoIqSymbol(index), self.tdtoIbSymbol(index), str(self.symbols[key].loc[index,"Description"])]],index=[index],columns=["Symbol","SymbolIb","Security Name"])
                        freqTable = pd.concat([freqTable,newRow])
                        missingSymbols[index] = key'''
                # Increment Count in Main Table
                freqTable.loc[freqTable.index.isin(list(countIndices)),colName] = 1
                '''for index, row in freqTable.iterrows():
                    if index in sym.index:
                        freqTable.loc[index,"Sector"] = self.symbols[key].loc[index,"Sector"]
                        freqTable.loc[index,"Industry"] = self.symbols[key].loc[index,"Industry"]
                        freqTable.loc[index,"Sub-Industry"] = self.symbols[key].loc[index,"Sub-Industry"]
                        freqTable.loc[index,colName] = 1'''
                     
        freqTable = freqTable.fillna(0)
        freqTable['asset_type'] = 'EQUITY'
        freqTable.reset_index(inplace=True)
        freqTable.rename(columns={'index':'SymbolTd'},inplace=True)
        self.td.db.createTable(freqTable,"tdEquityFrequencyTable",['"added" date default now()'] ,drop=True,)
        self.td.db.execute_mogrify(freqTable.values.tolist(),"tdEquityFrequencyTable",insertTables=['tdEquityFrequencyTable'])
        return
        
    def getTotalFreqTable(self,broker="Td",priceHist=False,fundamentals=False,fullMarket=False):
        '''
        Create table of symbols sorted by frequency in indices.
        broker -> (str) Either 'Td' (TD Ameritrade), 'Ib' \
                    (Interactive Brokers), or 'Iq' (IQ Feed) for which symbol \
                    style to use
        '''
        if broker=="Td":
            index = "SymbolTd"
        elif broker=="Ib":
            index = "SymbolIb"
        elif broker=="Iq":
            index = "Symbol"

        self.db.cur.execute(
            '''
                SELECT Max("dateadded")
                FROM PUBLIC."tdfundamentaldata"
            ''')

        startdate = self.db.cur.fetchone()[0]
        if startdate is None:
            startdate = self.today - datetime.timedelta(180)
            startdate = startdate.strftime('%Y-%m-%d %H:%M')
            self.log.info(startdate)

        try:
            colNames = [index,"freq"]
            # Get Total Frequency table (priceHist focuses on optionable and those in indices)
            if fundamentals:
                colNames = [index,"freq","Time","Event"]
                if self.today.isoweekday() == 5:
                    # Returns symbols whose earnings has recently occured or that will occur in the next three days
                    self.db.cur.execute(
                    '''
                        WITH X AS
                        (
                            SELECT 
                                "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                            FROM PUBLIC."tdequityfrequencytable"
                        ),Y AS (
                            SELECT 
                                row_number() OVER (PARTITION BY "Symbol" ORDER BY "Time"),"Time","Symbol","Event"
                            FROM PUBLIC."calendar_tdscan"
                            WHERE "Event"='Earnings'
                        )
                        SELECT 
                            t1."%s",t1."freq",t2."Time",t2."Event"
                        FROM X t1
                            LEFT JOIN Y t2
                                ON t1."%s"=t2."Symbol"
                        WHERE 
                            ("Time">='%s' 
                                AND 
                                ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP)
                            OR
                            (("Time" AT TIME ZONE 'America/New_York')       >=CURRENT_TIMESTAMP
                                AND
                                ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP + INTERVAL '3' DAY)
                            OR
                            "Time" is null
                        ORDER BY "Time","freq" DESC
                    ''' % (index,index,index,startdate))
                elif fullMarket:
                    self.db.cur.execute(
                    '''
                        WITH X AS
                        (
                            SELECT 
                                "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                            FROM PUBLIC."tdequityfrequencytable"
                        ),Y AS (
                            SELECT 
                                row_number() OVER (PARTITION BY "Symbol" ORDER BY "Time" DESC),"Time","Symbol","Event"
                            FROM PUBLIC."calendar_tdscan"
                            WHERE "Event"='Earnings'
                        )
                        SELECT 
                            t1."%s",t1."freq",t2."Time",t2."Event"
                        FROM X t1
                            LEFT JOIN Y t2
                                ON t1."%s"=t2."Symbol"
                        WHERE 
                            "row_number"=1
                            OR
                            "Time" is null
                        ORDER BY "Time","freq" DESC
                    ''' % (index,index,index))
                else:
                    # Returns symbols whose earnings has recently occured or that will occur in the next day
                    self.db.cur.execute(
                        '''
                            WITH X AS
                            (
                                SELECT 
                                    "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                                FROM PUBLIC."tdequityfrequencytable"
                            ),Y AS (
                                SELECT 
                                    row_number() OVER (PARTITION BY "Symbol" ORDER BY "Time"),"Time","Symbol","Event"
                                FROM PUBLIC."calendar_tdscan"
                                WHERE "Event"='Earnings'
                            )
                            SELECT 
                                t1."%s",t1."freq",t2."Time",t2."Event"
                            FROM X t1
                                LEFT JOIN Y t2
                                    ON t1."%s"=t2."Symbol"
                            WHERE 
                                ("Time">='%s' 
                                    AND 
                                    ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP)
                                OR
                                (("Time" AT TIME ZONE 'America/New_York')       >=CURRENT_TIMESTAMP
                                    AND
                                    ("Time" AT TIME ZONE 'America/New_York')<=CURRENT_TIMESTAMP + INTERVAL '1' DAY)
                                OR
                                "Time" is null
                            ORDER BY "Time","freq" DESC
                        ''' % (index,index,index,startdate))
            elif priceHist:
                # Returns Symbols that are optionable, movers, or are in major indices
                self.db.cur.execute(
                    '''
                        WITH X AS (
                            SELECT 
                                "%s","Dji","Nasd100","Optionable","PennyOptions","Russell","Sp400","Sp500","WeeklyOptions","Movers",
		                        "Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                            FROM PUBLIC."tdequityfrequencytable"
                            ORDER BY "freq" DESC
                        )
                        SELECT "%s","freq"
                        FROM X
                        WHERE "freq">0 
                            and ("Optionable"=1 or "Dji"=1
                                or "Nasd100"=1 or "Russell"=1
                                or "Sp400"=1 or "Sp500"=1  or "Movers"=1
                                or "PennyOptions"=1 or "WeeklyOptions"=1)
                    ''' % (index))
            else:
                # Returns every symbol in universe
                self.db.cur.execute(
                    '''
                        SELECT 
                            "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                        FROM PUBLIC."tdequityfrequencytable"
                        ORDER BY "freq" DESC,"%s"
                    ''' % (index,index))
            self.equityFreqTable = pd.DataFrame(self.db.cur.fetchall(),columns=colNames).set_index(index)
            self.log.info(self.equityFreqTable)
            self.db.conn.commit()
            
            # Get Optionable frequency table
            if not fundamentals:
                # Returns every symbol in universe that is optionable
                self.db.cur.execute(
                    '''
                    SELECT 
                        "%s","Dji"+"Nasd100"+"Optionable"+"PennyOptions"+"Russell"+"Sp400"+"Sp500"+"WeeklyOptions"+"Movers"+"Sectors" as "freq"
                    FROM PUBLIC."tdequityfrequencytable"
                    WHERE "Optionable" = 1
                    ORDER BY "freq" DESC,"%s"
                    ''' % (index,index))
                self.optionableFreqTable = pd.DataFrame(self.db.cur.fetchall(),columns=colNames).set_index(index)
                self.db.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.log.exception()
            self.db.conn.rollback()
        '''self.equityFreqTable = sqlio.read_sql_query('SELECT * FROM TDEQUITYFREQUENCYTABLE', self.connAlch,index_col=index)'''
        '''for colName in colNames:
            if (colName in self.db.bigint_list) or (colName in self.db.decimal_list):
                self.equityFreqTable[colName] = self.equityFreqTable[colName].astype(float)
        count = self.equityFreqTable.sum(axis=1,numeric_only=True)
        count = count[count != 0]
        count = count.to_frame("freq")
        count = count["freq"].sort_values(ascending=False)'''
        '''
        res = pd.DataFrame()
        res["freq"] = 0
        res["td_service_name"] = 'QUOTE'
        
        for index, value in count.iteritems():
            res.loc[index] = {'freq': value, 'td_service_name': 'QUOTE'}
        return res
        '''
        return
    
    def tdtoIbSymbol(self,tdSymbol=''):
        '''
        Function for converting TD Ameritrade symbol to Interactive Brokers
        tdSymbol -> Symbol to convert
        '''
        ibSymbol = tdSymbol.replace("/WS"," WAR",)
        ibSymbol = ibSymbol.replace("/U"," U")
        ibSymbol = ibSymbol.replace("p"," PR")
        return ibSymbol

    def tdtoIqSymbol(self,tdSymbol=''):
        '''
        Function for converting TD Ameritrade symbol to IQ Feed
        tdSymbol -> Symbol to convert
        '''
        iqSymbol = tdSymbol.replace("/",".")
        return iqSymbol

    def tdtoIqSeries(self,tdSeries=''):
        '''
        Function for converting Pandas Series of TD Ameritrade symbols to IQ Feed
        tdSeries -> Symbol to convert
        '''
        iqSeries = tdSeries.str.replace("/WS"," WAR",regex=False)
        iqSeries = iqSeries.str.replace("/U"," U",regex=False)
        iqSeries = iqSeries.str.replace("p"," PR",regex=False)
        return iqSeries

    def tdtoIbSeries(self,tdSeries=''):
        '''
        Function for converting Pandas Series of TD Ameritrade symbols to Interactive Brokers
        tdSeries -> Pandas Series of symbols to convert
        '''
        ibSeries = tdSeries.str.replace("/",".",regex=False)
        return ibSeries

    def connect(self):
        """
        Connect to the PostgreSQL database server
        """      
        try:
            # connect to the PostgreSQL server
            #self.log.debug('Connecting to the PostgreSQL database...')
            self.db = databaseHandler(self.params)
            #self.connAlch = self.db.connAlch
            #alchemyStr = f"postgresql+psycopg2://{self.params['user']}:{self.params['password']}@{self.params['host']}/{self.params['database']}?application_name={APP_NAME}_Main_Alchemy"
            #self.connAlch = create_engine(alchemyStr).connect()
            self.log.debug("Connection successful")
            return
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error(error)
            return