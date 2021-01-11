import logging
import requests
import azure.functions as func
import json


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    videoID = req.params.get('id')
    videoState = req.params.get('state')
    
    ## Only do stuff if the video has been processed
    if videoState != "Processed ":
        pass
    else:
        ## Create VideoIndexer object
        vi = VideoIndexer(
            vi_subscription_key=os.getenv("SUBSCRIPTION_KEY"),
            vi_location=os.getenv('LOCATION'),
            vi_account_id=os.getenv('ACCOUNT_ID'),
            block_blob_service=bbs,
            container_source=containerInput
        )
        ## Get base URL and params needed to make a requests
        url,params = vi.get_urlBase_and_params()
        ## Make request
        r = requests.get(f"{url}/Videos/{videoID}/Index",params=params)
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
        ## Create SQL query to run
        sqlQuery = create_sql_query(readyForSQL=listOfStringRows)