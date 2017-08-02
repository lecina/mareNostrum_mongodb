import argparse
from db import *

def __printExtendedHelp():
    print "\
    Description:\n\
    ------------\n\
    This program performs simple database operations to keep track of PELE++\n\
    simulation parameters.\n\n\
    The main operations are:\n\
    1) Storing data in a database, using MongoDB under the hood.\n\
    2) Querying and returning data from the database\n\n\
    Storing the data:\n\
    -----------------\n\
    The program will add to the database all the files that match the matching\n\
    pattern specified by \"-m\" in the list of root folders specified by \"-f\",\n\
    and its subsequent sub-folders. The files must be in JSON format.\n\
    This step is omitted if we don't specify any folder. The default matching\n\
    pattern is \"*.conf\".\n\n\
    The program uses MongoDB as the underlying database. It starts the server\n\
    and connects to the localhost, port 27017. If we want to start it manually,i\n\
    we can specify the no server option with \"-s\". Thei host can be changed\n\
    with the flag \"--host\".\n\
    The data is stored in a database named after the user. Each user can have\n\
    several collections. We specify the working one with the flag \"-c\".\n\
    The collection is dropped at the beginning, unless we specify that we\n\
    want to add the results to an existing one with the flag \"-a\".\n\n\
    Querying the data:\n\
    ------------------\n\
    The program uses the MongoDB query language. The query is specified\n\
    between single quotes after the flag \"-q\".\n\
    The results can be given in two ways using the flag \"--print-mode\":\n\
    filename: Prints the control filenames that match the query.\n\
    folder: Prints the containing folders of the matching control files.\n\
    By default it uses the latter option.\n\n\
    NOTE that the program does not recognise the default parameters in\n\
    a simulation. If the user omits a parameter in the control file, he/she\n\
    must be in charge of changing the query so that it meets his/her needs.\n\n\
    Examples:\n\
    ---------\n\
    1) python db.py -c aspirin -f ../simulations/ain* -q '{}'\n\n\
    Removes the previous \"aspirin\" collection and adds all the control files\n\
    in \"../simulations/ain*\" to it. Afterwards, it queries for all the documents\n\
    and prints the control file containing folders.\n\n\
    2)python db.py -c BEN -f ../simulations/ain* -m \"originalControlFile*\" -a\n\n\
    Adds the control files in \"../simulations/ain*\" that match the\n\
    \"originalControlFile*\" criterion to the \"BEN\" collection.\n\
    If it does not exist, it is created.\n\n\
    3)python db.py -q '{\"commands.Perturbation.parameters.temperature\":1000}'\n\n\
    Returns all the folders in the \"test\" collection (default one) that\n\
    contain a control file where the temperature is 1000 in the parameters\n\
    block inside the Perturbation\n\n\
    4)python db.py -q '{}' --print-mode filename\n\n\
    Prints all the control files in the \"test\" database.\n\
"

def __parseArguments():
    desc = "Program that performs simple database operations to keep\
            track of simulation parameters."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--host", default="localhost", 
                        help="Host to connect to the mongod instance.")
    parser.add_argument("-m", default="*.conf", metavar="MATCHING_PATTERN", 
                        help="Control file matching pattern.\
                        By default it uses \"*.conf\".") 
    parser.add_argument("-f", "--folders", action="store", nargs='+',
                        help="Root folder with the simulation results to add \
                        to the database. It visits new folders recursively.") 
    parser.add_argument("-a", action="store_true", help="Add the found control \
                        files to an existing database.") 
    parser.add_argument("-c", metavar="COLLECTION", default="test", 
                        help="Collection name")
    parser.add_argument("-q", "--query", default=None, type=json.loads,\
                        help="Query to perform in MongoDB query language.")
    parser.add_argument("--print-mode", default='folder', choices=['filename',
                        'folder'], help="Print mode")
    parser.add_argument("-e", "--extended-help", action="store_true",
                        help="Prints the extended help")
    parser.add_argument("-n", "--no-server", action="store_true",
                        help="Does not start the mongod process. The user must start it.")
    args = parser.parse_args()

    if args.extended_help:
        __printExtendedHelp()
        sys.exit()

    return  args.folders,\
            args.host,\
            args.m,\
            not args.a,\
            args.c,\
            args.query,\
            args.print_mode,\
            not args.no_server

def main():
    folders, host, matchingPattern, drop, collectionName, query, printMode, startMongod = __parseArguments()

    mongodProcess = startMongodIfAskedFor(startMongod, host)
    
    collection = openConnectionAndGetCollection(collectionName, host)

    if drop: dropCollection(collection)

    insertControlFiles(collection, folders, matchingPattern)

    foundFiles = queryDocuments(collection, query)

    results = getNames(foundFiles, printMode)

    if results:
        for result in results:
            print result

    terminateMongodProcess(mongodProcess)


if __name__ == '__main__':
    main() 
