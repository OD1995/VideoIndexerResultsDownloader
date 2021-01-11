from datetime import datetime, timedelta

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
            el['confidence'],
            ## Text
            el['text']
        ]
        for inst in el['instances']:
            ## Loop through all the instances of that block of text
            ##    (will probably only be one instance)
            listToAppend = textList + [
                ## Start time (of text mention)
                adjust_time(
                    time=inst['start'],
                    vidNumber=videoNumber
                )
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
    pass

def readyForSQL(
    readyForSQL
):
    Q = """
INSERT INTO REPLACE_ME
(VideoName, OriginalVideoName, DateTimeAdded, Accuracy, TextStartTime, TextEndTime, Text)
VALUES



    """


def adjust_time(
    time,
    vidNumber
):
    ## Get string into datetime object
    timeDT = datetime.strptime(time,'%H:%M:%S.%f')
    ## Add appropriate hours
    newTimeDT = timeDT + timedelta(hours=vidNumber-1)
    ## Get object back into string, trim off extra 4 microseconds digits
    returnMe = datetime.strftime(newTimeDT,'%H:%M:%S.%f')[:-4]

    return returnMe

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



def representsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False