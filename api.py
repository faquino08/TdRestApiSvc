import logging
from statistics import median_grouped
import sys
import datetime
import pytz
from os import path, environ
from urllib import request
import json
import argparse

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from flask import Flask, request, g
from flask_restful import Api
from flask_apscheduler import APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from constants import PROJECT_ROOT, POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, DEBUG
from database import db

from DataBroker.movers import movers
from DataBroker.quotes_options import quotes_options
from DataBroker.fundamentals import fundamentals
from DataBroker.symbolsUniverse import symbolsUniverse
from DataBroker.equity_freq import equity_freq
from DataBroker.pricehist import PriceHist

# Custom Convert
import werkzeug
from werkzeug.routing import PathConverter
from packaging import version

class EverythingConverter(PathConverter):
    regex = '.*?'

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

def create_app(db_location,debug=False):
    '''
    Function that creates our Flask application.
    This function creates the Flask app, Flask-Restful API,
    and Flask-SQLAlchemy connection
    db_location -> Connection string to the database
    debug -> Boolean of whether to log debug messages
    '''
    nyt = pytz.timezone('America/New_York')
    if DEBUG:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=nyt).date()}.txt'), logging.StreamHandler()],
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/tdameritradeFlask_{datetime.datetime.now(tz=nyt).date()}.txt'), logging.StreamHandler()],
        )
    logger = logging.getLogger(__name__)
    app = Flask(__name__)
    app.config.from_object(Config())
    app.url_map.converters['everything'] = EverythingConverter
    app.config["SQLALCHEMY_DATABASE_URI"] = db_location
    if environ.get("FLASK_ENV") == "production":
        app.config["SERVER_NAME"] = "172.21.34.8:5000"
    db.init_app(app)
    # initialize scheduler
    executors = {
        'default': ThreadPoolExecutor(1),
    }
    background = BackgroundScheduler(executors=executors)
    scheduler = APScheduler(scheduler=background)
    scheduler.init_app(app)
    params = {
            "host": f'{POSTGRES_LOCATION}',
            "port": f'{POSTGRES_PORT}',
            "database": f'{POSTGRES_DB}',
            "user": f'{POSTGRES_USER}',
            "password": f'{POSTGRES_PASSWORD}'
        }

    @scheduler.task('cron', id='symbols_universe', minute='0', hour='6', day_of_week='mon-fri', timezone='America/New_York')
    def runSymbolsUniverse():
        return symbolsUniverse(debug=DEBUG)

    @app.route('/run-symbol', methods=['POST'])
    def postSymbols():
        return json.dumps({
        'status':'success'
        }), symbolsUniverse(debug=DEBUG)

    @scheduler.task('cron', id='movers', minute='30', hour='17', day_of_week='mon-fri', timezone='America/New_York')
    def runMovers():
        return movers(debug=DEBUG)

    @app.route('/run-mover', methods=['POST'])
    def postMovers():
        return json.dumps({
        'status':'success'
        }), movers(debug=DEBUG)

    @scheduler.task('cron', id='quotes_options_optionable', minute='35', hour='17', day_of_week='mon-thu', timezone='America/New_York')
    def runQuotes_Options_Optionable():
        return quotes_options(debug=DEBUG)
    
    @scheduler.task('cron', id='quotes_options_full', minute='35', hour='17', day_of_week='fri', timezone='America/New_York')
    def runQuotes_Options_FullMarket():
        return quotes_options(debug=DEBUG,fullMarket=True)

    @app.route('/runquotes-options', methods=['POST'])
    def postQuotes_Options():
        reminder_delay = int(request.args['delay'])
        if len(request.args['fullMarket']) == 0:
            fullMarket = False
        else:
            fullMarket = json.loads(request.args['fullMarket'].lower())
        addQuotes_Options(scheduler, reminder_delay, fullMarket)
        return json.dumps({
        'status':'success',
        'delay': reminder_delay
        })

    @scheduler.task('cron', id='fundamentals_upcoming', minute='0', hour='22', day_of_week='mon-thu', timezone='America/New_York')
    def runFundamentals_Upcoming():
        return fundamentals(debug=DEBUG)
    
    @scheduler.task('cron', id='fundamentals_full', minute='30', hour='22', day_of_week='fri', timezone='America/New_York')
    def runFundamentals_FullMarket():
        return fundamentals(debug=DEBUG,fullMarket=True)

    @app.route('/run-fundamentals', methods=['POST'])
    def postFundamentals():
        reminder_delay = int(request.args['delay'])
        if len(request.args['fullMarket']) == 0:
            fullMarket = False
        else:
            fullMarket = json.loads(request.args['fullMarket'].lower())
        addFundamentals(scheduler, reminder_delay, fullMarket)
        return json.dumps({
        'status':'success',
        'delay': reminder_delay
        })

    @scheduler.task('cron', id='price_hist_optionable', minute='50', hour='00', day_of_week='tue-fri', timezone='America/New_York')
    def runPriceHist_Optionable():
        return priceHist(debug=DEBUG)

    @scheduler.task('cron', id='price_hist_full', minute='0', hour='3', day_of_week='sat', timezone='America/New_York')
    def runPriceHist_FullMarket():
        return priceHist(debug=DEBUG,fullMarket=True)

    @app.route('/runPriceHist', methods=['POST'])
    def postPriceHist():
        reminder_delay = int(request.args['delay'])
        if len(request.args['fullMarket']) == 0:
            fullMarket = False
        else:
            fullMarket = json.loads(request.args['fullMarket'].lower())
        addPriceHist(scheduler, reminder_delay, fullMarket)
        return json.dumps({
        'status':'success',
        'delay': reminder_delay
        })

    @app.route('/run_flow', methods=['POST'])
    def run_flow():
        reminder_delay = int(request.args['delay'])
        if len(request.args['fullMarket']) == 0:
            fullMarket = False
        else:
            fullMarket = json.loads(request.args['fullMarket'].lower())
        addWorkFlow(scheduler, reminder_delay, flow)
        return json.dumps({
        'status':'success',
        'delay': reminder_delay
        }), 

    @app.route('/runequityfreqtable', methods=['POST'])
    def runEquityFreqTable():
        reminder_delay = int(request.args['delay'])
        addEquityTable(scheduler,reminder_delay)
        return json.dumps({
        'status':'success'
        })

    def flow(fullMarket=False):
        runSymbolsUniverse()
        runMovers()
        if fullMarket:
            runQuotes_Options_FullMarket()
            runFundamentals_FullMarket()
            runPriceHist_FullMarket()
        else:
            runQuotes_Options_Optionable()
            runFundamentals_Upcoming()
            runPriceHist_Optionable()
        return
    
    scheduler.start()
    return app

def addQuotes_Options(scheduler, delay,fullMarket=False):
    '''
    Add quotes & options flow to AP Scheduler.
    scheduler -> APScheduler Object
    delay -> (int) Second to wait before running flow
    '''
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    job_id = 'manual_quotes_options'
    scheduler.add_job(id=job_id,func=quotes_options, trigger='date',\
        run_date=scheduled_time,kwargs={'debug':DEBUG,'fullMarket':fullMarket})
    logger.info('Quotes and Options Job Added')

def addPriceHist(scheduler,delay,fullMarket=False):
    '''
    Add price histoy flow to AP Scheduler.
    scheduler -> APScheduler Object
    delay -> (int) Second to wait before running flow
    '''
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    job_id = 'manual_PriceHist'
    scheduler.add_job(id=job_id,func=PriceHist, trigger='date',\
        run_date=scheduled_time,kwargs={'debug':DEBUG,'fullMarket':fullMarket})
    logger.info('Quotes and Options Job Added')

def addFundamentals(scheduler, delay, fullMarket=False):
    '''
    Add fundamentals flow to AP Scheduler.
    scheduler -> APScheduler Object
    delay -> (int) Second to wait before running flow
    '''
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    job_id = 'manual_fundamentals'
    scheduler.add_job(id=job_id,func=fundamentals, trigger='date',\
        run_date=scheduled_time,kwargs={'debug':DEBUG,'fullMarket':fullMarket})
    logger.info('Quotes and Options Job Added')

def addWorkFlow(scheduler, delay, flow, fullMarket=False):
    '''
    Add entire workflow to AP Scheduler.
    scheduler -> APScheduler Object
    delay -> (int) Second to wait before running flow
    workflow -> (function) Function running all the functions sequentially
    '''
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    job_id = 'manual_flow'
    scheduler.add_job(id=job_id,func=flow, trigger='date',\
        run_date=scheduled_time,kwargs={'debug':DEBUG,'fullMarket':fullMarket})
    logger.info('Daily Flow Job Added')

def addEquityTable(scheduler, delay):
    '''
    Add calculating frequency table flow to AP Scheduler.
    scheduler -> APScheduler Object
    delay -> (int) Second to wait before running flow
    workflow -> (function) Function running all the functions sequentially
    '''
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    job_id = 'manual_equitytable'
    scheduler.add_job(id=job_id,func=equity_freq, trigger='date',\
        run_date=scheduled_time,kwargs={'debug':DEBUG})
    logger.info('Equity Frequency Table Job Added')

app = create_app(f"postgresql://{PROJECT_ROOT}/{POSTGRES_DB}",False)