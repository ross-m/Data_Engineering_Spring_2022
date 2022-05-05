# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options
import io
import time
import psycopg2
import argparse
import re
import csv

unlogged = False
copy_from = True
DBname = "postgres"
DBuser = "postgres"
DBpwd = "password"
TableName = 'CensusData'
Datafile = "acs2015_census_tract_data_part1.csv"  # name of the data file to be loaded
CreateDB = True  # indicates whether the DB table should be (re)-created


def row2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # ENHANCE: handle the null vals
        row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

    ret = f"""
       {row['CensusTract']},            -- CensusTract
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop
       {row['Men']},                    -- Men
       {row['Women']},                  -- Women
       {row['Hispanic']},               -- Hispanic
       {row['White']},                  -- White
       {row['Black']},                  -- Black
       {row['Native']},                 -- Native
       {row['Asian']},                  -- Asian
       {row['Pacific']},                -- Pacific
       {row['Citizen']},                -- Citizen
       {row['Income']},                 -- Income
       {row['IncomeErr']},              -- IncomeErr
       {row['IncomePerCap']},           -- IncomePerCap
       {row['IncomePerCapErr']},        -- IncomePerCapErr
       {row['Poverty']},                -- Poverty
       {row['ChildPoverty']},           -- ChildPoverty
       {row['Professional']},           -- Professional
       {row['Service']},                -- Service
       {row['Office']},                 -- Office
       {row['Construction']},           -- Construction
       {row['Production']},             -- Production
       {row['Drive']},                  -- Drive
       {row['Carpool']},                -- Carpool
       {row['Transit']},                -- Transit
       {row['Walk']},                   -- Walk
       {row['OtherTransp']},            -- OtherTransp
       {row['WorkAtHome']},             -- WorkAtHome
       {row['MeanCommute']},            -- MeanCommute
       {row['Employed']},               -- Employed
       {row['PrivateWork']},            -- PrivateWork
       {row['PublicWork']},             -- PublicWork
       {row['SelfEmployed']},           -- SelfEmployed
       {row['FamilyWork']},             -- FamilyWork
       {row['Unemployment']}            -- Unemployment
    """

    return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

# read the input data file into a list of row strings
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)

        rowlist = []
        for row in dr:
            rowlist.append(row)

    return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
    cmdlist = []
    for row in rowlist:
        valstr = row2vals(row)
        cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
        cmdlist.append(cmd)
    return cmdlist

# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = False
    return connection

def createUnloggedTable(conn):
    TableName = 'CensusDataUnlogged'
    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName};
            CREATE UNLOGGED TABLE {TableName} (
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );	
        """)

        print(f"Created {TableName}")

def appendFromTable(conn):
    createTable(conn)
    with conn.cursor() as cursor:
        cursor.execute(f"""
            INSERT INTO CensusData SELECT * FROM CensusDataUnlogged;
        """)

        print(f"Created {TableName}")

def clean_csv_value(value):
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):
    TableName = 'CensusData'
    fil = readdata(Datafile)
    rlis = io.StringIO()
    for dp in fil:
        rlis.write('|'.join(map(clean_csv_value, (
                dp['CensusTract'],
                dp['State'],
                dp['County'],
                dp['TotalPop'],
                dp['Men'],
                dp['Women'],
                dp['Hispanic'],
                dp['White'],
                dp['Black'],
                dp['Native'],
                dp['Asian'],
                dp['Pacific'],
                dp['Citizen'],
                dp['Income'],
                dp['IncomeErr'],
                dp['IncomePerCap'],
                dp['IncomePerCapErr'],
                dp['Poverty'],
                dp['ChildPoverty'],
                dp['Professional'],
                dp['Service'],
                dp['Office'],
                dp['Construction'],
                dp['Production'],
                dp['Drive'],
                dp['Carpool'],
                dp['Transit'],
                dp['Walk'],
                dp['OtherTransp'],
                dp['WorkAtHome'],
                dp['MeanCommute'],
                dp['Employed'],
                dp['PrivateWork'],
                dp['PublicWork'],
                dp['SelfEmployed'],
                dp['FamilyWork'],
                dp['Unemployment']
        ))) + '\n')

    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName};
            CREATE TABLE {TableName} (
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );	
            ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
            CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)
        start = time.perf_counter()
        cursor.copy_from(rlis, 'censusdata', sep='|');
        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

        print(f"Created {TableName}")

def load(conn, icmdlist):

    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()

        for cmd in icmdlist:
            cursor.execute(cmd)

        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
    if copy_from:
        start = time.perf_counter()
        conn = dbconnect()
        if CreateDB:
            createTable(conn)

        elapsed = time.perf_counter() - start

    elif unlogged:
        conn = dbconnect()
        rlis = readdata(Datafile)
        cmdlist = getSQLcmnds(rlis)

        if CreateDB:
            createUnloggedTable(conn)

        load(conn, cmdlist)

        appendFromTable(conn)

        TableName = 'CensusData'

        with conn.cursor() as cursor:
            cursor.execute(f"""
                ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
                CREATE INDEX idx_{TableName}_State ON {TableName}(State);
            """)
    
    else:
        initialize()
        conn = dbconnect()
        rlis = readdata(Datafile)
        cmdlist = getSQLcmnds(rlis)

        if CreateDB:
            createTable(conn)

        load(conn, cmdlist)

        with conn.cursor() as cursor:
            cursor.execute(f"""
                ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
                CREATE INDEX idx_{TableName}_State ON {TableName}(State);
            """)


if __name__ == "__main__":
    main()



