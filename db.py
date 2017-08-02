import sys
import os
import fnmatch
import json
import getpass
import subprocess
import time
import socket


#TODO: add write concern?
#TODO: think what to do with different users running mongod
#Put: __ in front of "private" functions


sys.path.append("/home/bsc72/bsc72755/lib/pymongo/lib/python/pymongo-2.6.3-py2.7-linux-x86_64.egg")
import pymongo

DEFAULT_MONGOD = "/gpfs/scratch/bsc72/bsc72755/mongodb/bin/mongod"
DEFAULT_DBPATH = "/gpfs/projects/bsc72/bsc72755/db"
DEFAULT_LOGPATH = "/gpfs/projects/bsc72/bsc72755/log/log.txt"

mongod =  os.environ.get("mongod", DEFAULT_MONGOD)
dbpath = os.environ.get("dbpath", DEFAULT_DBPATH)
logpath = os.environ.get("logpath", DEFAULT_LOGPATH)


DEFAULT_COLLECTION_NAME = 'test'


def findAllFilesInFolderWithMatchingPattern(folders, matchingPattern):
    """
        Function that returns all the control files that match 
        "matchingPattern" in "folders" and recursive folders.
    """
    filesToAdd = [] 

    if not folders:
       folders = []

    for folder in folders: 
        for root, dirnames, filenames in os.walk(folder):
            for filename in fnmatch.filter(filenames, matchingPattern):
                path = os.path.join(os.getcwd(), root, filename)
                absolutePath = os.path.abspath(path)
                filesToAdd.append(absolutePath)

    return filesToAdd


def openConnectionAndGetCollection(collectionName=DEFAULT_COLLECTION_NAME, host='localhost'):
    """
        Opens a connection with "host" and returns a
        collection named "collectionName" in the db
        with the user's name. Assumes mongod is ready
    """
    try:
        connection = pymongo.MongoClient(host, w=1, wtimeout=250)
    except:
        print 'Could not connect to mongod'
        return None
    
    #The database name is equal to the user name
    databaseName = getpass.getuser()
    db = connection[databaseName]
    collection = db[collectionName]

    return collection


def insertDocuments(filenames, collection):
    """
        Adds the documents  "filenames" to the collection.
    """
    if not collection or not filenames:
        return

    for filename in filenames:
        file = open(filename, 'r')
        controlFile = file.read()
        file.close()
        try:
            controlFile = json.loads(controlFile)
        except Exception as ex:
            print "Exception occurred: %s with file %s" % (str(ex),filename)
            continue
        try:
            addFolderAndFilenameAsField(controlFile, filename)
            collection.insert(controlFile)
        except Exception as ex:
            print "Exception occurred: %s" % str(ex)

def addFolderAndFilenameAsField(controlFile, filename):
    k = filename.rfind('/')
    containingFolder = filename[:k]
    controlFile['__folder'] = containingFolder
    controlFile['__filename'] = filename[k+1:]

def insertControlFiles(collection, folders=["./"], matchingPattern="*.conf"):
    """
        Adds all the files in folders that match "matchingPattern" to the collection
    """

    filesToAdd = findAllFilesInFolderWithMatchingPattern(folders, matchingPattern)
    insertDocuments(filesToAdd, collection)

def queryDocuments(collection, query):
    #Not the same as if not query: return [],
    #since {} is False, but returns all the collection
    if collection != None and query != None:
        try:
            return collection.find(query)
        except:
            return []

    return []

def getFolderFromDocument(doc):
    return doc['__folder']

def getFullFilenameFromDocument(doc):
    return os.path.join(doc['__folder'], doc['__filename'])

def getMapFunction(printMode):
    if printMode == 'filename':
        return getFullFilenameFromDocument
    elif printMode == 'folder':
        return getFolderFromDocument
    else:
        raise Exception('Print mode not found')

def getNamesFromFiles(mapFunction, foundDocuments):
    names = map(mapFunction, foundDocuments)
    names = set(names)

    return names

def getNames(foundDocuments, resultMode):
    mapFunction = getMapFunction(resultMode)

    return getNamesFromFiles(mapFunction, foundDocuments)

def startMongodIfAskedFor(startMongod, host):
    if startMongod:
        return runMongodInstance(host)
    else:
        return None

def runMongodInstance(host='localhost'):
    try:
        args = [mongod, "--dbpath", dbpath, "--logpath", logpath, "--logappend"]
        mongodProcess = subprocess.Popen(args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    except OSError as e:
        sys.exit('Could not find mongod')

    validConnection = waitForMongodToBeReady(mongodProcess, host)

    if not validConnection:
       raise NameError('Mongod is still not ready!') 

    return mongodProcess

def waitForMongodToBeReady(mongodProcess, host):
    timeout = 100
    while mongodProcess.poll() is None and timeout > 0:
        timeout -= 1 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect((host, 27017))
                return True
            except (IOError, socket.error):
                time.sleep(0.1)
        finally:
            s.close()

    return False

def terminateMongodProcess(mongodProcess):
    if not mongodProcess:
        return

    try:
        mongodProcess.terminate()
        mongodProcess.wait()
    except:
        pass

def dropCollection(collection):
    try:
        collection.drop()
    except:
        pass
