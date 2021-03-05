from datetime import datetime, timedelta
import os
import pyodbc
import logging
import pandas as pd

def sqlise_tl(
    transcript_list,
    videoName,
    origVideoName,
    videoAdded,
    videoNumber
):
    returnMe = []

    for el in transcript_list:
        ## Create list to be added as a SQL row
        ##    - this is for each unique block of text
        textList = [
            ## Video name
            videoName,
            ## Original video name - Video name without our “1ofX” add-on
            origVideoName,
            ## DateTime of when it was added
            videoAdded,
            ## Accuracy
            float(el['confidence']),
            ## Text
            el['text'].replace("'","''")
        ]
        for inst in el['instances']:
            ## Loop through all the instances of that block of text
            ##    (will probably only be one instance)
            listToAppend = textList + [
                ## Start time (of text mention)
                adjust_time(
                    time=inst['start'],
                    vidNumber=videoNumber
                ),
                ## End time (of text mention)
                adjust_time(
                    time=inst['end'],
                    vidNumber=videoNumber
                )
                ]
            rowAsString = SQLise_list(listToAppend)
            returnMe.append(rowAsString)

    return returnMe

def SQLise_list(
    listOfValues
):
    ## Do the appropriate formating (mostly adding "'" to start and end)
    _list_ = [
        ## Video name
        f"'{listOfValues[0]}'",
        ## Original video name - Video name without our “1ofX” add-on
        f"'{listOfValues[1]}'",
        ## DateTime of when it was added
        f"""'{listOfValues[2].split(".")[0]}.{listOfValues[2].split(".")[1][:3]}'""",
        ## Accuracy
        str(listOfValues[3]),
        ## Text
        f"'{listOfValues[4]}'",
        ## Start time (of text mention)
        f"'{listOfValues[5]}'",
        ## End time (of text mention)
        f"'{listOfValues[6]}'",
    ]
    ## Join with commas
    return ",".join(_list_)

def adjust_time(
    time,
    vidNumber
):
    if time == '0:00:00':
        _format_ = '%H:%M:%S'
    else:
        _format_ = '%H:%M:%S.%f'
    ## Get string into datetime object
    try:
        timeDT = datetime.strptime(time,_format_)
    except ValueError:
        if "." in time:
            time2 = "".join(time.split(".")[:-1])
        else:
            time2 = time
        timeDT = datetime.strptime(time2,'%H:%M:%S')
    ## Add appropriate hours
    newTimeDT = timeDT + timedelta(hours=vidNumber-1)
    ## Get object back into string, trim off extra 4 microseconds digits
    returnMe = datetime.strftime(newTimeDT,'%H:%M:%S.%f')[:-4]

    return returnMe

def create_sql_query(
    readyForSQL
):
    seperator = ')\n,('
    Q = f"""
INSERT INTO VideoIndexerTranscripts
(VideoName, OriginalVideoName, DateTimeAdded, Accuracy, Text, TextStartTime, TextEndTime)
VALUES
({seperator.join(readyForSQL)})
    """

    return Q

def get_vid_name_info(
    videoName
):
    ## # Work out if there's a (for example) "1of5_" at the start
    ## Test 1 - at least one underscore present
    underscore_present = videoName.count("_") > 0
    ## Test 2 - pre-underscore container "of"
    preUnderscore = videoName.split("_")[0]
    of_in_preUnderscore = "in" in preUnderscore
    ## If both are True, move onto Test 3
    if underscore_present & of_in_preUnderscore:
        ## Test 3 - before and after "of" are numbers
        before,after = preUnderscore.split("of")
        before_isNumber = representsInt(before)
        after_isNumber = representsInt(after)
        ## If Test 3 passed, we have the answer
        if before_isNumber & after_isNumber:
            origVideoName = "".join(videoName.split("_")[1:])
            videoNumber = int(before)
        else:
            origVideoName = videoName
            videoNumber = 1          
    else:
        origVideoName = videoName
        videoNumber = 1

    return origVideoName,videoNumber


def get_connection_string():
    ## Get information used to create connection string
    username = 'matt.shepherd'
    password = os.getenv("sqlPassword")
    driver = '{ODBC Driver 17 for SQL Server}'
    server = os.getenv("sqlServer")
    database = 'AzureCognitive'
    ## Create connection string
    return f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

def run_sql_query(query):
    connectionString = get_connection_string()
    logging.info(f"connectionString: {connectionString}")
    ## Execute query
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            logging.info("About to execute 'INSERT' query")
            cursor.execute(query)
            logging.info("'INSERT' query executed")

def get_VideoIndexerIDs_dict():
    connectionString = get_connection_string()
    with pyodbc.connect(connectionString) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(
            sql="SELECT * FROM VideoIndexerIDs",
            con=conn
        )
    return dict(
        zip(
            df.VideoID,
            df.FileURL
        )
    )

def representsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def get_container_from_URL(fileURL):
    return fileURL.split("/")[3]

def get_file_name_from_URL(fileURL):
    return "/".join(fileURL.split("/")[4:])

def get_url_container_and_file_name(fileURL):
    return get_container_from_URL(fileURL),get_file_name_from_URL(fileURL)