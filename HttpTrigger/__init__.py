import logging
import requests
import azure.functions as func
import json
from MyFunctions import get_vid_name_info, sqlise_tl, create_sql_query, run_sql_query
from MyClasses import VideoIndexer
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    videoID = req.params.get('id')
    videoState = req.params.get('state')
    logging.info(f"videoID: {videoID}")
    logging.info(f"videoState: {videoState}")
    
    ## Only do stuff if the video has been processed
    if videoState != "Processed":
        logging.info("File not processed")
    else:
        ## Create VideoIndexer object
        vi = VideoIndexer(
            vi_subscription_key=os.getenv("SUBSCRIPTION_KEY"),
            vi_location=os.getenv('LOCATION'),
            vi_account_id=os.getenv('ACCOUNT_ID')
        )
        ## Get base URL and params needed to make a requests
        url,params = vi.get_urlBase_and_params()
        ## Make request
        r = requests.get(f"{url}/Videos/{videoID}/Index",params=params)
        logging.info("request made")
        ## Get response
        js = json.loads(r.text)
        ## Get info from json
        transcript_list = js['videos'][0]['insights']['transcript']
        videoName = js['name']
        origVideoName,videoNumber = get_vid_name_info(videoName)
        videoAdded = js['created']
        ## Convert to SQLised list of lists
        listOfStringRows = sqlise_tl(
            transcript_list=transcript_list,
            videoName=videoName,
            origVideoName=origVideoName,
            videoAdded=videoAdded,
            videoNumber=videoNumber
        )
        logging.info("listOfStringRows created")
        ## Create SQL query to run
        sqlQuery = create_sql_query(readyForSQL=listOfStringRows)
        logging.info("sqlQuery created")
        ## Run (INSERT) query
        run_sql_query(sqlQuery)
        logging.info("sqlQuery run")