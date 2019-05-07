#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
PURPOSE:
Harvest metadata from a number of data centres contributing to GCW and
other activities, prepare metadata for ingestion.

Following a request from GCW Steering Group, the number of ingested
records is monitored. Distinguish between active and deleted records.

AUTHOR:
    Øystein Godøy, METNO/FOU, 2014-01-05 
        Original Perl variant.

UPDATED:
    Øystein Godøy, METNO/FOU, 2018-05-10 
        Modified to Python

COMMENTS:
    - Add logging
    - Add selective harvest (interactive based on config)
    - Add temporal span for OAI-PMH harvest (missing for CSW)

"""

import sys
import os
import getopt
import yaml
from harvest_metadata3 import MetadataHarvester

# import the necessary packages
import argparse
from pathlib import Path
import json
import parmap


# construct the argument parse and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-c", "--config", required=True,
                help="specify the configuration file to use")
ap.add_argument("-l", "--logfile", required=False,
                help="specify the logfile to use")
ap.add_argument("-f", "--from", required=False,
                help="specify DateTime to harvest from")
args = vars(ap.parse_args())


def usage():
    print(sys.argv[0]+" [options] input")
    print("\t-h|--help: dump this information")
    print("\t-c|--config: specify the configuration file to use")
    print("\t-l|--logfile: specify the logfile to use")
    print("\t-f|--from: specify DateTime to harvest from")
    sys.exit(2)


# consider the use of pathlib
def check_directories(cfg):
    for section in cfg:
        for name in ['raw','mmd']:
            # print section, name
            # #print cfg[section][name]
            if not os.path.isdir(cfg[section][name]):
                try:
                    os.makedirs(cfg[section][name])
                except:
                    print("Could not create output directory")
                    return 2
    return 0


# consider the use of pathlib
def check_directories2(cfg):
    for section in cfg:
        for name in ['raw', 'mmd']:
            # print section, name
            # #print cfg[section][name]
            if Path(cfg[section][name]).is_dir():
                print("Using existing directory %s" % cfg[section][name])
            else:
                try:
                    Path(cfg[section][name]).mkdir(parents=False)
                    print("Create directory %s" % cfg[section][name])
                except FileNotFoundError:
                    print("Could not create output directory: %s " % cfg[section][name])
                    return 2
    return 0

def main2():
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", required=True,
                    help="specify the configuration file to use")
    ap.add_argument("-l", "--logfile", required=False,
                    help="specify the logfile to use")
    ap.add_argument("-f", "--from", required=False,
                    help="specify DateTime to harvest from")
    args = vars(ap.parse_args())
    cflg = lflg = fflg = False
    if args['config']:
        print(args['config'])
        cfgfile = args['config']
        cflg = True
    if args['logfile']:
        print(args['logfile'])
        lflg = True
    if args['from']:
        print(args['from'])
        fflg = True

    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # Check that all relevant directories exists...
    if check_directories2(cfg):
        print("Something went wrong creating directories")
        sys.exit(2)
    # Each section is a data centre to harvest
    allreq = {}
    allreq['record'] = []
    for section in cfg:
        if section == 'CCIN':
            continue
        if cfg[section]['protocol'] == 'OAI-PMH':
            if cfg[section]['set']:
                if fflg:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix="+cfg[section]['mdkw'] + \
                            "&set=" + cfg[section]['set'] + \
                            "&from=" + fromTime
                else:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix=" + cfg[section]['mdkw'] + \
                            "&set=" + cfg[section]['set']
            else:
                if fflg:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix=" + cfg[section]['mdkw'] + \
                            "&from=" + fromTime
                else:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix=" + cfg[section]['mdkw']
        elif cfg[section]['protocol'] == 'OGC-CSW':
            request = "?SERVICE=CSW&VERSION=2.0.2"\
                    "&request=GetRecords&constraintLanguage=CQL_TEXT" \
                    "&typeNames=csw:Record"\
                    "&resultType=results"\
                    "&outputSchema=http://www.isotc211.org/2005/gmd&elementSetName=full"
            req = {}
            req['baseURL'] = cfg[section]['source']
            req['records'] = request
            req['outputDir'] = cfg[section]['raw']
            req['hProtocol'] = cfg[section]['protocol']
            req['section'] = section
        #srcfmt = None, username = None, pw = None
            allreq['record'].append(req)
        else:
            print("Protocol not supported yet")
        #print(request)
        numRec = 0
        #mh = MetadataHarvester(cfg[section]['source'],
        #        request, cfg[section]['raw'],
        #        cfg[section]['protocol'],
        #        cfg[section]['mdkw'])
        #try:
        #    numRec = mh.harvest()
        #except Exception as e:
        #    print("Something went wrong on harvest from", section)
        #    print(str(e))
        #print("Number of records harvested", section, numRec)
        #print(allreq)
    with open('data.txt', 'w') as outfile:
        json.dump(allreq, outfile)
    #with open('data.txt') as json_file:
    #    data = json.load(json_file)
    #parmap.map(runReq, allreq['record'], pm_processes=4, pm_pbar=True)

    for i in allreq['record']:
        try:
            runReq(i)
        except:
            print('failed at request {0}'.format(i))

    #runReq(data['record'][0])


def runReq(request):
    mh = MetadataHarvester(baseURL=request['baseURL'],
               records=request['records'],
               outputDir=request['outputDir'],
               hProtocol=request['hProtocol'])
    numRec = mh.harvest()
    print("Number of records harvested", request['section'], numRec)
    # MetadataHarvester(baseURL, records, outputDir, hProtocol, srcfmt=None, username=None, pw=None)

###########################################################
def main(argv):
    # Parse command line arguments
    try:
        opts, args = getopt.getopt(argv, "hc:l:f:",
                ["help", "config", "logfile", "from"])
    except getopt.GetoptError:
        usage()

    cflg = lflg = fflg = False
    for opt, arg in opts:
        if opt == ("-h", "--help"):
            usage()
        elif opt in ("-c", "--config"):
            cfgfile = arg
            cflg = True
        elif opt in ("-l", "--logfile"):
            logfile = arg
            lflg = True
        elif opt in ("-f", "--from"):
            fromTime = arg
            fflg = True

    if not cflg:
        usage()
    elif not lflg:
        usage()

    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # Check that all relevant directories exists...
    if check_directories(cfg):
        print("Something went wrong creating directories")
        sys.exit(2)

    # Each section is a data centre to harvest
    allreq = {}
    allreq['record'] = []
    for section in cfg:
        if section == 'CCIN':
            continue
        if cfg[section]['protocol'] == 'OAI-PMH':
            if cfg[section]['set']:
                if fflg:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix="+cfg[section]['mdkw'] + \
                            "&set=" + cfg[section]['set'] + \
                            "&from=" + fromTime
                else:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix=" + cfg[section]['mdkw'] + \
                            "&set=" + cfg[section]['set']
            else:
                if fflg:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix=" + cfg[section]['mdkw'] + \
                            "&from=" + fromTime
                else:
                    request = "?verb=ListRecords"\
                            "&metadataPrefix=" + cfg[section]['mdkw']
        elif cfg[section]['protocol'] == 'OGC-CSW':
            request = "?SERVICE=CSW&VERSION=2.0.2"\
                    "&request=GetRecords&constraintLanguage=CQL_TEXT" \
                    "&typeNames=csw:Record"\
                    "&resultType=results"\
                    "&outputSchema=http://www.isotc211.org/2005/gmd&elementSetName=full"
        else:
            print("Protocol not supported yet")
        print(request)
        #numRec = 0
        #mh = MetadataHarvester(cfg[section]['source'],
        #        request, cfg[section]['raw'],
        #        cfg[section]['protocol'],
        #        cfg[section]['mdkw'])
        #try:
        #    numRec = mh.harvest()
        #except Exception as e:
        #    print("Something went wrong on harvest from", section)
        #    print(str(e))
        #print("Number of records harvested", section, numRec)

        req = {}
        req['baseURL'] = cfg[section]['source']
        req['records'] = request
        req['outputDir'] = cfg[section]['raw']
        req['hProtocol'] = cfg[section]['protocol']
        req['section'] = section
        # srcfmt = None, username = None, pw = None
        allreq['record'].append(req)


    for i in allreq['record']:
        numRec = 0
        mh = MetadataHarvester(i['baseURL'], i['records'], i['outputDir'], i['hProtocol'])
        try:
            numRec = mh.harvest()
        except Exception as e:
            print("Something went wrong on harvest from", i['section'])
            print(str(e))
        print("Number of records harvested", i['section'], numRec)




    sys.exit(0)



def tryit(i):
    # parmap.map(runReq, allreq['record'], pm_processes=4, pm_pbar=True)
    mh = MetadataHarvester(i['baseURL'], i['records'], i['outputDir'], i['hProtocol'])
    try:
        numRec = mh.harvest()
    except Exception as e:
        print("Something went wrong on harvest from", section)
        print(str(e))

if __name__ == '__main__':
    #main(sys.argv[1:])
    main2()

