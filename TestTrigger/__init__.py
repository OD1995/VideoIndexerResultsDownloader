import logging
from MyFunctions import run_sql_query
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    Q = """
    INSERT INTO VideoIndexerIDs (VideoID,FileURL)
    VALUES ('testID1','testURL1')
    """
    logging.info(Q)
    run_sql_query(Q)
    logging.info("query ran")
    return func.HttpResponse("Done.")

