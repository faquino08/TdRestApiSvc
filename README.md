# TdQuoteFlaskDocker

Flask app that exposes an api for running workflows that get data from the TD Ameritrade REST API. Runs scheduled workflows to get price history, fundamental, and quote data.

# Docker Reference

The following is a description of each env var key and value:

**Key Name:** PROJECT_ROOT \
**Description:** a string containing the authentication information for the postgres server. DO NOT INCLUDE DATABASE NAME. \
**Values:** <span style="color:#6C8EEF">user:password@host:port</span>

**Key Name:** POSTGRES_DB \
**Description:** a string containing the name of the postgres database for data insertion. \
**Values:** <span style="color:#6C8EEF">\<postgres database name string></span>

**Key Name:** POSTGRES_USER \
**Description:**  a string containing the username the postgres server to use for authentication. \
**Values:** <span style="color:#6C8EEF">\<postgres username string></span>

**Key Name:** POSTGRES_PASSWORD \
**Description:** a string containing the password the postgres user specified. \
**Values:** <span style="color:#6C8EEF">\<postgres password string></span>

**Key Name:** POSTGRES_LOCATION \
**Description:** a string containing the hostname for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres hostname string></span>

**Key Name:** POSTGRES_PORT \
**Description:** a string containing the port for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres port string></span>

**Key Name:** DEBUG_BOOL \
**Description:** a string determining whether logging should include debug level messages. \
**Values:** <span style="color:#6C8EEF">True|False</span>

# Api Reference

[comment]: <> (First Command)
### <span style="color:#6C8EEF">**POST**</span> /run-symbol
Run workflow for retrieving listed symbols from NASDAQ ftp server. The server is update with new symbols around 10PM ET.

[comment]: <> (Second Command)
### <span style="color:#6C8EEF">**POST**</span> /run-mover
Run workflow for retrieving the day's biggest movers from TD Ameritrade REST API.

[comment]: <> (Third Command)
### <span style="color:#6C8EEF">**POST**</span> /runquotes-options?delay=<span style="color:#a29bfe">**:int**</span>&fullMarket=<span style="color:#a29bfe">**:boolean**</span>
Run workflow for retrieving end-of-day (EOD) quotes and options chain data.

#### **Arguments:**
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***10***
- **fullMarket** - should the price history be retrieved for all listed equities or just the ones that are in major indices/optionable? *Default:* ***False***

[comment]: <> (Fourth Command)
### <span style="color:#6C8EEF">**POST**</span> /run-fundamentals?delay=<span style="color:#a29bfe">**:int**</span>&fullMarket=<span style="color:#a29bfe">**:boolean**</span>
Run workflow for retrieving EOD fundamental data.

#### **Arguments:**
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***10***
- **fullMarket** - should the price history be retrieved for all listed equities or just the ones that are in major indices/optionable? *Default:* ***False***

[comment]: <> (Fifth Command)
### <span style="color:#6C8EEF">**POST**</span> /runPriceHist?delay=<span style="color:#a29bfe">**:int**</span>&fullMarket=<span style="color:#a29bfe">**:boolean**</span>
Run workflow for retrieving EOD fundamental data.

#### **Arguments:**
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***10***
- **fullMarket** - should the price history be retrieved for all listed equities or just the ones that are in major indices/optionable? *Default:* ***False***

[comment]: <> (Sixth Command)
### <span style="color:#6C8EEF">**POST**</span> /run_flow?delay=<span style="color:#a29bfe">**:int**</span>&fullMarket=<span style="color:#a29bfe">**:boolean**</span>
Runs the following flows in order:
- `/run-symbol`
- `/run-mover`
- `/runquotes-options`
- `/run-fundamentals`
- `/runPriceHist`

#### **Arguments:**
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***10***
- **fullMarket** - should the price history be retrieved for all listed equities or just the ones that are in major indices/optionable? *Default:* ***False***

[comment]: <> (Seventh Command)
### <span style="color:#6C8EEF">**POST**</span> /runequityfreqtable?delay=<span style="color:#a29bfe">**:int**</span>
Runs a workflow to compile the datafrom the `/run_symbol`, [TdScannerReader](../TdScannerReader/README.md#post-run-tos-readerdelayint)`/run-tos-reader` and [TdScannerReader](../TdScannerReader/README.md#post-run-tos-readerdelayint)`/run-sector-reader` workflows. It compiles them into a single table in Postgres detailing which equities belong to which indices and their respective sectors, industries, and sub-industries.

#### **Arguments:**
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***10***
- **fullMarket** - should the price history be retrieved for all listed equities or just the ones that are in major indices/optionable? *Default:* ***False***
