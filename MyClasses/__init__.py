import re
import time
import requests
from azure.storage.blob import ContainerPermissions
from datetime import datetime, timedelta
import logging


def get_retry_after_from_message(message):
    match = re.search(r'Try again in (\d+) second', message or '')
    if match:
        return int(match.group(1))

    return 30  # default to retry in 30 seconds

def get_SAS_URL(fileURL,
                block_blob_service,
                container):

    sasTokenRead = block_blob_service.generate_container_shared_access_signature(
    container_name=container,
    permission=ContainerPermissions.READ,
    expiry=datetime.utcnow() + timedelta(days=1)
    )
    return f"{fileURL}?{sasTokenRead}"


class VideoIndexer():
    def __init__(self,
                 vi_subscription_key,
                 vi_location,
                 vi_account_id,
                 block_blob_service,
                 container_source):
        self.vi_subscription_key = vi_subscription_key
        self.vi_location = vi_location
        self.vi_account_id = vi_account_id
        self.access_token = None
        self.block_blob_service = block_blob_service
        self.container_source = container_source

    def get_access_token(self):
        logging.info('Getting video indexer access token...')
        headers = {
            'Ocp-Apim-Subscription-Key': self.vi_subscription_key
        }

        params = {
            'allowEdit': 'true'
        }
        access_token_req = requests.get(
            'https://api.videoindexer.ai/auth/{loc}/Accounts/{acc_id}/AccessToken'.format(
                loc=self.vi_location,
                acc_id=self.vi_account_id
            ),
            params=params,
            headers=headers
        )

        access_token = access_token_req.text[1:-1]
        logging.info('Access Token: {}'.format(access_token))
        self.access_token = access_token
        return access_token
    def get_SAS_URL_method(self,fileURL):
        return get_SAS_URL(fileURL=fileURL,
                           block_blob_service=self.block_blob_service,
                           container=self.container_source)

    def check_access_token(self):
        if not self.access_token:
            self.get_access_token()

    def upload_to_video_indexer(self, video_url, video_name, video_language='English'):
        self.check_access_token()

        logging.info('Uploading video to video indexer...')
        
        ## Get SAS URL
        sasURL = get_SAS_URL(fileURL=video_url,
                             block_blob_service=self.block_blob_service,
                             container=self.container_source)
        
        params = {
            'streamingPreset': 'Default',
            'indexingPreset': 'DefaultWithNoiseReduction',
            'language': video_language,
            'name': video_name,
            'accessToken': self.access_token,
            'videoUrl' : sasURL,
            'callbackUrl' : "https://futuresvideoindexerresultsdownloader.azurewebsites.net/api/HttpTrigger"
        }

        request_url = 'https://api.videoindexer.ai/{loc}/Accounts/{acc_id}/Videos'.format(
                                            loc=self.vi_location,
                                            acc_id=self.vi_account_id
                                        )
        
        upload_video_req = self.do_requesting(request_url=request_url,
                                              params=params)
        

        return upload_video_req
    
    def get_info_back(self, videoID, _type_):
        self.check_access_token()
        
        params = {'type' : _type_,
                  'accessToken': self.access_token}
        
        request_url = 'https://api.videoindexer.ai/{loc}/Accounts/{acc_id}/Videos/{videoId}/ArtifactUrl'.format(
                                            loc=self.vi_location,
                                            acc_id=self.vi_account_id,
                                            videoId=videoID
                                        )
        info_req = self.do_requesting(request_url=request_url,
                                              params=params)
        

        return info_req
    
    def get_urlBase_and_params(self):
        self.check_access_token()
        
        a = 'https://api.videoindexer.ai/{loc}/Accounts/{acc_id}'.format(
                                            loc=self.vi_location,
                                            acc_id=self.vi_account_id
                                        )
        b = {'accessToken': self.access_token}
        
        return a,b
    
    def do_requesting(self, request_url,params):
        self.check_access_token()

        retry_count = 5
        while True:
            if retry_count < 1:
                raise Exception('Retry count exceeded.')

            upload_video_req = requests.post(
                url=request_url,
                params=params
            )

            if upload_video_req.status_code == 200:
                break

            if upload_video_req.status_code == 429:  # hit throttling limit, sleep and retry
                error_resp = upload_video_req.json()
                logging.info('Throttling limit hit. Error message: {}'.format(error_resp.get('message')))
                retry_after = get_retry_after_from_message(error_resp.get('message'))
                time.sleep(retry_after + 1)
                retry_count -= 1
                continue
            raise Exception('Error uploading video to video indexer')

        return upload_video_req