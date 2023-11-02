import os
import logging
import inspect
from typing import Union
from ftplib import FTP
import time
import pytz
import pandas as pd
from datetime import timedelta, datetime, timezone
import psycopg2
import psycopg2.extras
import os
from tdQuoteFlaskDocker.constants import DEBUG
from  DataBroker.Sources.TDAmeritrade.database import databaseHandler

logger = logging.getLogger(__name__)

params = {
    "host": '',
    "port": '',
    "database": '',
    "user": '',
    "password": ''
}

class SymbolsUniverse:
    def __init__(self,postgres: Union[databaseHandler,dict],debug=False,insert=False,startTime=0):
        '''
        Class to grab US symbols universe from nasdaq five days a week.
        postgresParams -> Dict with keys host, port, database, user, \
                            password for Postgres database
        debug   -> Whether to record debug logs
        insert  -> Whether to insert data into database
        startTime   -> (int) Start time of workflow in Epoch   
        '''
        self.nyt = pytz.timezone('America/New_York')
        if logger != None:
            self.log = logger
        else:
            loggingLvl = logging.DEBUG if DEBUG else logging.INFO
            logging.basicConfig(
                level=loggingLvl,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M:%S",
                handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=self.nyt).date()}.txt'), logging.StreamHandler()],
            )
        
        if type(postgres) == databaseHandler:
            self.db = postgres
        else:
            if not all(x in postgres for x in params):
                raise ValueError(f'SymbolsUniverse did not receive a valid value for postgres. Need to receive either an object of type {databaseHandler} or dict in format {params}')
            self.postgres = postgres
            self.db = databaseHandler(self.postgres)

        # Files to Download
        self.files_list = [
            "nasdaqlisted.txt",
            "otherlisted.txt",
            "mpidlist.txt",
            "mfundslist.txt"
        ]

        # Lists of columns for each file type
        # char_list -> varchar(1) etc
        self.char_list = [
            "Market Category",
            "Test Issue",
            "Financial Status",
            "Exchange",
            "ETF",
            "NextShares",
            "Pending",
            "Options Closing Type",
            "Options Type",
            "MP Type",
            "NASDAQ Member",
            "FINRA Member",
            "NASDAQ BX Member *",
            "PSX Participant",
            "Category",
        ]
        self.chartwo_list =[
            "Type",
        ]
        self.symbol_list = [
            "ACT Symbol",
            "CQS Symbol",
            "Root Symbol",
            "Underlying Symbol",
            "SymbolTd",
            "SymbolIB"
        ]
        self.uniqueSymbol_list = [
            "Symbol",
            "NASDAQ Symbol",
            "MPID",
            "Fund Symbol"
        ]
        self.int_list = [
            
        ]
        self.double_list = [
            "Explicit Strike Price",
            "Round Lot",
            "Round Lot Size"
        ]
        self.bigint_list = [
            "Volume",
            "Market Cap",
        ]
        self.decimal_list = [
            "Last",
            "Vol Index",
            "Net Chng",
            "%Change",
            "Bid",
            "Ask",
            "High",
            "Low",
            "EPS",
            "Dji",
            "Nasd100",
            "Optionable",
            "PennyOptions",
            "Russell",
            "Sp400",
            "Sp500",
            "WeeklyOptions",
            "Movers",
            "Sectors"
        ]
        self.name_list = [
            "Security Name",
            "Underlying Issue Name",
            "Name",
            "Location",
            "Phone Number",
            "Fund Name",
            "Fund Family Name",
            "Pricing Agent"
        ]
        self.date_list = [
            "Expiration Date"
        ]
        self.create_list = [
            "Added"
        ]
        self.mod_list = [
            "Updated"
        ]
        
        caller = 'TD' + inspect.stack()[1][3].upper()
        # Create New Run in RunHistory
        self.db.cur.execute('''
            INSERT INTO PUBLIC.financedb_RUNHISTORY ("Process","Startime","SymbolsToFetch") VALUES ('%s','%s',1) RETURNING "Id";
        ''' % (caller,startTime))
        self.runId = self.db.cur.fetchone()[0]
        self.db.conn.commit()
        # Run
        self.main()

    def main(self):
        '''
        Main Function to retrieve files from NASDAQ FTP Server and insert into Postgres Database.
        '''
        # Connect to Nasdaq
        nas = FTP('nasdaqtrader.com')
        nas.login()
        nas.cwd('/symboldirectory')
        list = nas.retrlines('LIST',self.listLineCallback)
        self.log.debug(list)

        for f in self.files_list:
            if os.path.exists(f):
                self.log.info(f + ': already exists')
            else:
                lf = open(f, "wb")
                nas.retrbinary('RETR %s' % f, lf.write, 8*1024)
                lf.close()

        # Close FTP Connection
        self.log.info("Quit FTP")
        nas.quit()

        for fi in self.files_list:
            # Create Table
            self.today = datetime.now(tz=self.nyt).date()
            self.yesterday = self.today - timedelta(days=1)
            table = fi[:-4]
            filename = fi[:-4] + ".txt"

            self.copy_from_stringio(filename, table)

        return

    def exit(self):
        '''
        Exit class and close Postgres connection
        '''
        self.log.debug('Beginning of exit()')
        try:
            self.endTime = time.time()
            # Update RunHistory With EndTime
            self.db.cur.execute('''
                UPDATE PUBLIC.financedb_RUNHISTORY
                SET "Endtime"=%s,
                    "SymbolsInsert"=%s,
                    "LastSymbol"='%s'
                WHERE "Id"=%s
            ''' % (self.endTime,0,'NULL',self.runId))
            self.db.conn.commit()
            self.db.exit()
            '''self.cur.close()
            self.conn.close()
            self.connAlch.close()
            self.log.info('Db Exit Status:')
            self.log.info('Psycopg2:')
            self.log.info(self.conn.closed)
            self.log.info('SqlAlchemy:')
            self.log.info(self.connAlch.closed)'''
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error(error)
        return

    def listLineCallback(self,line):
        '''
        Log list of files on NASDAQ FTP Server
        '''
        msg = "** %s*"%line
        self.log.info(msg)

    def colSql(self,panda=pd.DataFrame(),\
        dateType="timestamp without time zone",createCol=True):
        '''
        Takes pandas dataframe and returns string formatting the columns for SQL Create Table query.
        datetype -> (str) Type of Date for added column
        createCol -> (boolean) Create added column
        '''
        res = "("
        # Loop Through Columns and Append to
        for col in panda.columns:
            if col in self.symbol_list:
                res += "%s varchar(14)," % '"{}"'.format(col)
            elif col in self.uniqueSymbol_list:
                res += "%s varchar(14) primary key," % '"{}"'.format(col)
            elif col in self.name_list :
                res += "%s varchar(255)," % '"{}"'.format(col)
            elif col in self.char_list:
                res += "%s varchar(1)," % '"{}"'.format(col)
            elif col in self.chartwo_list:
                res += "%s varchar(2)," % '"{}"'.format(col)
            elif col in self.int_list:
                res += "%s integer," % '"{}"'.format(col)
            elif col in self.date_list:
                res += "%s date," % '"{}"'.format(col)
            elif col in self.double_list:
                res += "%s double precision," % '"{}"'.format(col)     
            elif col in self.mod_list:
                res += "%s %s," % ('"{}"'.format(col),dateType)
            else:
                res += "%s text," % '"{}"'.format(col)
        if createCol == True:
            res += "\"Added\" %s default now()," % dateType
        res = res[:-1]
        res += ")"
        return res
    
    def getColNames(self,panda,unique=True,excluded=[],addlCols=[]) -> str:
        '''
        Takes pandas dataframe and returns string formatting the columns for SQL Select Table query.
        unique -> (boolean) excludes the unique columns from the response \
                        when set to False
        excluded -> (list) Columns from Dataframe to exclude from Sql query
        addlCols -> (list) Additional columns to insert on top of Dataframe\ 
                        columns
        '''
        res = "("
        # Loop Through Columns and Append to
        for col in panda.columns:
            notExcluded = bool(col not in excluded)
            if (notExcluded):
                if col in self.symbol_list:
                    res += "%s," % '"{}"'.format(col)
                elif col in self.uniqueSymbol_list and unique:
                    res += "%s," % '"{}"'.format(col)
                elif col in self.name_list :
                    res += "%s," % '"{}"'.format(col)
                elif col in self.char_list:
                    res += "%s," % '"{}"'.format(col)
                elif col in self.chartwo_list:
                    res += "%s," % '"{}"'.format(col)
                elif col in self.bigint_list:
                    res += "%s," % '"{}"'.format(col)
                elif col in self.date_list:
                    res += "%s," % '"{}"'.format(col)
                elif col in self.decimal_list:
                    res += "%s," % '"{}"'.format(col)
                else:
                    res += "%s," % '"{}"'.format(col)
        if len(addlCols) > 0:
            for col in addlCols:
                notExcluded = bool(col not in excluded)
                if (notExcluded):
                    res += "%s," % '"{}"'.format(col)
        res = str(res[:-1]) + ")"
        return res
    
    def getExcludedColNames(self, panda, unique=False,excluded=[],addlCols=[]) -> str:
        '''
        Get column names with excluded prepended to the names.
        panda -> (Dataframe) columns to get for command
        unique -> (boolean) excludes the unique columns from the response\
                         when set to False.
        excluded -> (list) list of columns to exclude
        addlCols -> (list) additional columns to include
        '''
        res = "("
        # Loop Through Columns and Append to
        for col in panda.columns:
            notExcluded = bool(col not in excluded)
            if (notExcluded):
                if col in self.symbol_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.uniqueSymbol_list and unique:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.name_list :
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.char_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.chartwo_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.int_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.date_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.double_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.create_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                elif col in self.mod_list:
                    res += "excluded.%s," % '"{}"'.format(col)
                else:
                    res += "excluded.%s," % '"{}"'.format(col)
        if len(addlCols) > 0:
            for col in addlCols:
                notExcluded = bool(col not in excluded)
                if (notExcluded):
                    res += "%s," % '"{}"'.format(col)
        res = str(res[:-1]) + ")"
        return res
    
    def execute_values(self, df=pd.DataFrame(), table='', excluded=[]):
        '''
        Takes dataframe and inserts the data into the provided table using execute_values.
        df -> (Dataframe) Dataframe to insert
        table -> (str) Name of Postgres Table
        '''
        # Get Lists of Column Names in different forms
        pKeys = {
            "otherlisted":"NASDAQ Symbol",
            "nasdaqlisted":"Symbol",
            "mpidlist":"MPID",
            "mfundslist":"Fund Symbol",
            "listedsymbols":"Symbol"
        }
        if table in pKeys.keys():
            self.log.info(f'{table.capitalize()} Duplicates:')
            self.log.info(str(df.loc[df.duplicated([pKeys[table]])]))
            df = df.drop_duplicates([pKeys[table]])
            colNames = self.getColNames(df)
            colNamesNoPK = self.getColNames(df,False,excluded=excluded) # Get Column Names without Primary Key
            excludedColNames = self.getExcludedColNames(df,excluded=excluded)
            # Create a list of tuples from the dataframe values
            tuples = [tuple(x) for x in df.to_numpy()]
            self.log.debug(tuples[0])
            self.log.debug(colNames)
            self.log.debug(excludedColNames)
            '''if table == 'listedsymbols':
                colNamesNoPK = colNamesNoPK[:-1] + ',"NASDAQ Symbol")'
                excludedColNames = excludedColNames[:-1] + ',excluded."NASDAQ Symbol")'''
            insert_sql = f'''
                INSERT INTO {table} {colNames} \
                    VALUES %s \
                    ON CONFLICT ("{pKeys[table]}") DO UPDATE SET \
                    {colNamesNoPK} = {excludedColNames};
            '''
            self.log.debug(str(insert_sql))
        #if("otherlisted" in table):
        #    insert_sql = f'''
        #        INSERT INTO {table} {colNames} \
        #            VALUES %s \
        #            ON CONFLICT ("NASDAQ Symbol") DO UPDATE SET \
        #            {colNamesNoPK} = {excludedColNames};
        #    '''
        #elif("nasdaqlisted" in table):
        #    insert_sql = f'''
        #        INSERT INTO {table} {colNames} \
        #            VALUES %s \
        #            ON CONFLICT ("Symbol") DO UPDATE SET \
        #            {colNamesNoPK} = {excludedColNames};
        #    '''
        #elif("mpidlist" in table):
        #    insert_sql = f'''
        #        INSERT INTO {table} {colNames}
        #        VALUES %s
        #        ON CONFLICT ("MPID") DO UPDATE SET \
        #            {colNamesNoPK} = {excludedColNames};
        #    '''
        #elif("mfundslist" in table):
        #    insert_sql = f'''
        #        INSERT INTO {table} {colNames}
        #        VALUES %s
        #        ON CONFLICT ("Fund Symbol") DO UPDATE SET \
        #            {colNamesNoPK} = {excludedColNames};
        #    '''
        #elif("listedsymbols" in table):
        #    self.log.debug(colNamesNoPK)
        #    insert_sql = f'''
        #        INSERT INTO {table} {colNames}
        #        VALUES %s \
        #        ON CONFLICT ("Symbol") DO UPDATE SET \
        #        {colNamesNoPK} = {excludedColNames};
        #    '''
        else:
            self.log.error("Table not Recognized")
            return 1
        try:
            psycopg2.extras.execute_values(self.db.cur, insert_sql, tuples)
            self.db.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.info("Error: %s" % error)
            self.db.conn.rollback()
            return 1
        self.log.info("execute_values() done")
        return
    
    def copy_from_stringio(self, file='', table=''):
        """
        Read txt file and insert into Postgres Table.
        file -> csv file to read
        table -> Table Name in Postgres
        """
        # Open file
        f = open(file, 'r')
        pan = pd.read_csv(file, sep="|",na_filter= False)
        pan = pan[:-1]
        f.close()

        # Set Update Timestamp
        pan["Updated"] = datetime.now(tz=timezone(-timedelta(hours=5)))\
            .isoformat()

        # Create Table
        cols = self.colSql(pan,createCol=True)
        create_sql = f"CREATE TABLE IF NOT EXISTS {table}{cols};"
        self.log.debug(create_sql)
        try:
            self.db.cur.execute(create_sql)
            self.db.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.db.conn.rollback()

        self.execute_values(pan,table)
        if os.path.exists(file):
            os.remove(file)
        else:
            self.log.info(f"The file: {file} does not exist") 
        self.log.info("copy_from_stringio() done")
        return

    def getListedSymbols(self):
        '''
        Get NASDAQ listed symbols and symbols listed on other US exchanges from Postgres Database.
        '''
        if self.today.isoweekday() == 1:
            filterDay = "CURRENT_DATE - INTERVAL '3 DAYS'"
        else: 
            filterDay = "CURRENT_DATE"
        nasdaqListedSql = 'SELECT  "Symbol", "ETF", "Round Lot Size", "Market Category" AS "Market", "Test Issue", "Security Name" FROM \"nasdaqlisted\" WHERE "Test Issue"=\'N\' AND "Updated" >= %s;' % filterDay
        otherListedSql = 'SELECT  "CQS Symbol" AS "Symbol", "NASDAQ Symbol", "ETF", "Round Lot Size", "Exchange" AS "Market", "Test Issue", "Security Name" FROM "otherlisted" WHERE "Test Issue"=\'N\' AND "Updated" >= %s;' % filterDay
        
        try:
            colNames = ["Symbol", "ETF", "Round Lot Size", "Market", "Test Issue", "Security Name"]
            self.db.cur.execute(nasdaqListedSql)
            self.nasdaqSymbols = pd.DataFrame(self.db.cur.fetchall(),columns=colNames)
            self.db.conn.commit()
            colNames = ["Symbol", "NASDAQ Symbol", "ETF", "Round Lot Size", "Market", "Test Issue", "Security Name"]
            self.db.cur.execute(otherListedSql)
            self.otherSymbols = pd.DataFrame(self.db.cur.fetchall(),columns=colNames)
            self.db.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.db.conn.rollback()
        '''self.nasdaqSymbols = sqlio.read_sql_query(nasdaqListedSql,con=self.connAlch)
        self.otherSymbols = sqlio.read_sql_query(otherListedSql,con=self.connAlch)'''
        
        self.setBrokerSymbols()
        return

    def setBrokerSymbols(self):
        '''
        Create Symbols columns for TD Ameritrade and Interactive Brokers and convert symbols.
        '''
        # Nasdaq Symbols for Brokers: Td Ameritrade and Interactive Brokers
        self.nasdaqSymbols['SymbolTd'] = self.nasdaqSymbols['Symbol']
        self.nasdaqSymbols['SymbolIb'] = self.nasdaqSymbols['Symbol']

        # Other Symbols for Brokers: Td Ameritrade and Interactive Brokers
        # Td Ameritrade
        self.otherSymbols['SymbolTd'] = self.otherSymbols['Symbol'].str.replace(".","/",regex=False)
        # Interactive Brokers
        self.otherSymbols['SymbolIb'] = self.otherSymbols['Symbol'].str.replace(".WS"," WAR",regex=False)
        self.otherSymbols['SymbolIb'] = self.otherSymbols['SymbolIb'].str.replace(".U"," U",regex=False)
        self.otherSymbols['SymbolIb'] = self.otherSymbols['SymbolIb'].str.replace("p"," PR",regex=False)
        self.createListedSymbols("listedsymbols")
        self.insertListedSymbols("listedsymbols")
        return
    
    def createListedSymbols(self,table):
        '''
        Create table in database for universe of symbols
        table -> (str) Name of Postgres Table
        '''
        # Drop Old Table
        '''drop_sql = f"DROP TABLE IF EXISTS {table};"
        self.log.debug(drop_sql)
        try:
            self.cur.execute(drop_sql)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.conn.rollback()'''
        # Create Table
        #listedCols = self.otherSymbols
        #listedCols['NASDAQ Symbol'] = ''
        self.otherSymbols['Updated'] = ''
        cols = self.colSql(self.otherSymbols,createCol=True)
        #Nasdaq Symbol can't be the primary key in this table because it includes companies not listed on Nasdaq
        cols = cols.replace(',"NASDAQ Symbol" varchar(14) primary key,',',"NASDAQ Symbol" varchar(14),')
        create_sql = f"CREATE TABLE IF NOT EXISTS \"{table}\"{cols};"
        self.log.debug(create_sql)
        try:
            self.db.cur.execute(create_sql)
            self.db.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.log.error("Error: %s" % error)
            self.db.conn.rollback()
        return
        
    def insertListedSymbols(self,table):
        '''
        Inserted both NASDAQ listed symbols and other US exchange listed symbols into Postgres.
        table -> (str) Name of Postgres Table
        '''
        self.nasdaqSymbols['NASDAQ Symbol'] = self.nasdaqSymbols['Symbol'].str[:]
        allSymbols = pd.concat([self.otherSymbols,self.nasdaqSymbols])
        allSymbols['Updated'] = datetime.now().astimezone(self.nyt)\
            .isoformat()
        allSymbols = allSymbols[["Symbol", "ETF", "Round Lot Size", "Market", "Test Issue", "Security Name", "SymbolTd", "SymbolIb", "Updated", "NASDAQ Symbol"]]
        self.log.debug(allSymbols.keys())
        self.log.debug(allSymbols)
        self.execute_values(allSymbols,table)
        return 