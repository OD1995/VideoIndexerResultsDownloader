import logging
import requests
import azure.functions as func
import json
from MyFunctions import (
    get_vid_name_info,
    sqlise_tl,
    create_sql_query,
    run_sql_query,
    get_VideoIndexerIDs_dict,
    get_url_container_and_file_name,
    get_VideoIndexerUploads_rows
)
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

        if videoState == "Failed":
            try:
                ## Get fileURL for the Video Indexer ID
                fileURL = get_VideoIndexerIDs_dict()[videoID]
                ## Check how many times it has been uploaded already (max 3)
                viuDF = get_VideoIndexerUploads_rows(fileURL)
                if len(viuDF) < 3:
                    ## If process failed, trigger the upload again
                    container,blob = get_url_container_and_file_name(fileURL)
                    data = {
                        'fileUrl' : fileURL,
                        'container' : container,
                        'blob' : blob
                    }
                    r = requests.post(
                        "https://futuresvideoindexeruploader.azurewebsites.net/api/HttpTrigger",
                        params=data
                    )
                    logging.info("sent off URL to go through VI again")
                else:
                    ## Add row to AzureProblems table
                    apQ = f"""
                    INSERT INTO AzureProblems (ProblemArea,ProblemDetails)
                    VALUES ('VideoIndexerUploaderMaxRetry','{fileURL}')
                    """
                    run_sql_query(apQ)
                    logging.info("File has been uploaded too many times already, row added to AzureProblems")

            except IndexError:
                logging.info("Video Indexer ID not in the VideoIndexerIDs SQL table")

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
        transcript_list_all = js['videos'][0]['insights']['transcript']
        ## Split transcript_list_all into lists of 500 (max insert is 1000)
        n = 500
        transcript_list_blocks = [
            transcript_list_all[i * n:(i + 1) * n]
            for i in range((len(transcript_list_all) + n - 1) // n )
        ]
        videoName = js['name']
        origVideoName,videoNumber = get_vid_name_info(videoName)
        videoAdded = js['created']
        logging.info(f"{len(transcript_list_blocks)} insert command to run")
        for transcript_list in transcript_list_blocks:
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

    return func.HttpResponse("done")