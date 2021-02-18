
# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import psycopg2.extras
import argparse
import re
import csv
import io
from collections import OrderedDict 

DBname = "repalceme"
DBuser = "repalceme"
DBpwd = "repalceme"
TableName = 'CensusData'
Datafile = "acs2015_census_tract_data.csv"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015

def format_row(row):
  # handle the null vals
  for key in row:
    if not row[key]:
      row[key] = 0
    row['County'] = row['County'].replace('\'','')  # eliminate quotes within literals
  return row  

def row2vals(row):
  row = format_row(row)
  ret = f"""
       {Year},                          -- Year
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

def row2dict(row, append_year=False):
  row = format_row(row)
  if append_year:
    row['Year'] = Year
  return row

def initialize():
  global Year

  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  parser.add_argument("-y", "--year", default=Year)
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable
  Year = args.year

# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
  print(f"readdata: reading from File: {fname}")
  with open(fname, mode="r") as fil:
    dr = csv.DictReader(fil)
    headerRow = next(dr)
    # print(f"Header: {headerRow}")

    rowlist = []
    for row in dr:
      rowlist.append(row)

  return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist, unlogged, temp):
  cmdlist = []
  name = TableName
  if unlogged:
    name = "stage"
  if temp:
    name = "temp"
  for row in rowlist:
    valstr = row2vals(row)
    cmd = f"INSERT INTO {name} VALUES ({valstr});"
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

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn, unlogged=False, temp=False, temp_buf=8000000):
  unlogged_stmt, temp_stmt, temp_buf_stmt = "", "", ""
  name = TableName
  if unlogged:
    unlogged_stmt = "UNLOGGED"
    name = "stage"
  if temp:
    temp_stmt = "TEMPORARY"
    name = "temp"
    temp_buf_stmt = f"SET temp_buffers={temp_buf}"
  with conn.cursor() as cursor:
    cursor.execute(f"""
          DROP TABLE IF EXISTS {name};
          {temp_buf_stmt};
          CREATE {unlogged_stmt} {temp_stmt} TABLE {name} (
              Year                INTEGER,
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

    print(f"Created {name}")

def load(conn, icmdlist, unlogged=False, temp=False, temp_buf=8000000):
  if unlogged or temp:  # create unlogged table or temporary table accordingly
    createTable(conn, unlogged=unlogged, temp=temp, temp_buf=temp_buf)

  with conn.cursor() as cursor:
    print(f"Loading {len(icmdlist)} rows")
    start = time.perf_counter()
    
    for cmd in icmdlist:
      print(cmd)
      cursor.execute(cmd)
                
    table = ""
    if unlogged:
      table = "stage"
    if temp:
      table = "temp"
    if unlogged or temp:
      cursor.execute(f"""
        INSERT INTO {TableName}
          (SELECT * FROM {table});
      """)

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

    cursor.execute(f"""
      ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract);
      CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    """)

def load_batch(conn, rowlist):
  with conn.cursor() as cursor:
    rowlist = [ dict(row2dict(row, append_year=True)) for row in rowlist ]
    print(f"Loading {len(rowlist)} rows")

    start = time.perf_counter()

    psycopg2.extras.execute_batch(cursor, f"""
      INSERT INTO {TableName} VALUES (
        %(Year)s,
        %(CensusTract)s,
        %(State)s,
        %(County)s,
        %(TotalPop)s,
        %(Men)s,
        %(Women)s,
        %(Hispanic)s,
        %(White)s,
        %(Black)s,
        %(Native)s,
        %(Asian)s,
        %(Pacific)s,
        %(Citizen)s,
        %(Income)s,
        %(IncomeErr)s,
        %(IncomePerCap)s,
        %(IncomePerCapErr)s,
        %(Poverty)s,
        %(ChildPoverty)s,
        %(Professional)s,
        %(Service)s,
        %(Office)s,
        %(Construction)s,
        %(Production)s,
        %(Drive)s,
        %(Carpool)s,
        %(Transit)s,
        %(Walk)s,
        %(OtherTransp)s,
        %(WorkAtHome)s,
        %(MeanCommute)s,
        %(Employed)s,
        %(PrivateWork)s,
        %(PublicWork)s,
        %(SelfEmployed)s,
        %(FamilyWork)s,
        %(Unemployment)s
      );
      """, rowlist)

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

    cursor.execute(f"""
      ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract);
      CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    """)

def load_by_copy_from(conn, rowlist):
  def clean_csv_value(value) -> str:
    if value is None:
      return r'\N'
    return str(value).replace('\n', '\\n')

  with conn.cursor() as cursor:
    rowlist = [ 
      OrderedDict(
        list(OrderedDict([('Year', Year)]).items()) 
        + list(row2dict(row).items())
      ) for row in rowlist 
    ]
    print(f"Loading {len(rowlist)} rows")

    start = time.perf_counter()

    csv_file_like_object = io.StringIO()
    for row in rowlist:
      csv_file_like_object.write(
        '|'.join(map(clean_csv_value, [row[key] for key in row])) + '\n')
    csv_file_like_object.seek(0)
    cursor.copy_from(csv_file_like_object, TableName, sep='|')

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

    cursor.execute(f"""
      ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract);
      CREATE INDEX idx_{TableName}_State ON {TableName}(State);
    """)


def main():
    # flags
    execute_in_batch     = False
    execute_by_copy_from = True
    unlogged_table       = False
    temporary_table      = False
    
    # variables
    temporary_table_buffer_size = 256000000

    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)

    if CreateDB:
      createTable(conn)

    if execute_by_copy_from:
      load_by_copy_from(conn, rlis)
    elif execute_in_batch:
      load_batch(conn, rlis)
    else:
      cmdlist = getSQLcmnds(
        rlis, unlogged=unlogged_table, temp=temporary_table)
      load(
        conn, 
        cmdlist, 
        unlogged=unlogged_table, 
        temp=temporary_table, 
        temp_buf=temporary_table_buffer_size)


if __name__ == "__main__":
    main()
