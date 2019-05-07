# Each section is a data centre to harvest
for section in cfg:
    if section == 'CCIN':
        continue
    if cfg[section]['protocol'] == 'OAI-PMH':
        if cfg[section]['set']:
            if fflg:
                request = "?verb=ListRecords" \
                          "&metadataPrefix=" + cfg[section]['mdkw'] + \
                          "&set=" + cfg[section]['set'] + \
                          "&from=" + fromTime
            else:
                request = "?verb=ListRecords" \
                          "&metadataPrefix=" + cfg[section]['mdkw'] + \
                          "&set=" + cfg[section]['set']
        else:
            if fflg:
                request = "?verb=ListRecords" \
                          "&metadataPrefix=" + cfg[section]['mdkw'] + \
                          "&from=" + fromTime
            else:
                request = "?verb=ListRecords" \
                          "&metadataPrefix=" + cfg[section]['mdkw']
    elif cfg[section]['protocol'] == 'OGC-CSW':
        request = "?SERVICE=CSW&VERSION=2.0.2" \
                  "&request=GetRecords&constraintLanguage=CQL_TEXT" \
                  "&typeNames=csw:Record" \
                  "&resultType=results" \
                  "&outputSchema=http://www.isotc211.org/2005/gmd&elementSetName=full"
    else:
        print("Protocol not supported yet")
    print(request)
    numRec = 0
    mh = MetadataHarvester(cfg[section]['source'],
                           request, cfg[section]['raw'],
                           cfg[section]['protocol'],
                           cfg[section]['mdkw'])
    try:
        numRec = mh.harvest()
    except Exception as e:
        print("Something went wrong on harvest from", section)
        print(str(e))
    print("Number of records harvested", section, numRec)
