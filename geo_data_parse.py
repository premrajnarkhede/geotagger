#!/usr/bin/env python
from __future__ import print_function, division
import argparse
import json
import sys
from gzip import GzipFile
from bz2 import BZ2File
from collections import defaultdict
from unidecode import unidecode
from multiprocessing.dummy import Pool as ThreadPool

pool = ThreadPool(64)

def concat_claims(claims):
    for rel_id, rel_claims in claims.iteritems():
        for claim in rel_claims:
            yield claim

label_dict = {}
def to_triplets(line):
    try:
        ent = json.loads(line.rstrip('\n,'))
    except Exception,e:
        print(e)
        return None
    if not ent['id'].startswith('Q'):
        print("Skipping item with id {}".format(ent['id']),
              file=sys.stderr)
        return None
    
    triplets = []
    e1 = ent['id']
    label = ent["labels"].get("en",{}).get("value","")
    alias = ent["aliases"].get("en",[])
    alias =  [a.get("value","") for a in alias if a["value"]!=""]
    if label == "":
        #print("returned")
        return None
    allowed_properties = {"P1082":"population","P31":"instance","P30":"continent","P17":"country","P36":"capital","P2046":"area","P625":"location"}
    record = defaultdict(list)
    record["label"].append(label)
    record["alias"].extend(alias)
    record["ent"].append(e1)
    flag = 0
    main = 0
    try:
        if "P1082" in ent["claims"] or "P36" in ent["claims"] or "P2046" in ent["claims"]:
            main = 1

    except Exception,e:
        print(ent["claims"])
        print(e)
        return None
    if main == 0:
        return None
    flag = 1
    claims = concat_claims(ent['claims'])
    for claim in claims:
        mainsnak = claim['mainsnak']
        if mainsnak['property'] not in allowed_properties:
            continue
        if mainsnak['snaktype'] != "value":
            continue
        if mainsnak['datatype'] == 'wikibase-item':
            rel = mainsnak['property']
            e2 = 'Q{}'.format(mainsnak['datavalue']['value']['numeric-id'])
            record[allowed_properties[mainsnak['property']]].append(e2)
        elif mainsnak['datatype'] == 'string' or mainsnak['datatype'] == 'url':
            rel = mainsnak['property']
            e2 = mainsnak['datavalue']['value']
            record[allowed_properties[mainsnak['property']]].append(e2)
        else:
            record[allowed_properties[mainsnak['property']]].append(str(mainsnak))
    if flag == 1:
        return record
def readCSV(path):
    import csv
    reader = csv.reader(open(path,'rU'), delimiter=',', quotechar='"')
    return reader

def writeCSV(path,mode="w"):
    import unicodecsv
    myfile=open(path,mode)
    fileOutput = unicodecsv.writer(myfile, delimiter=',',quotechar='"',lineterminator='\n')
    return fileOutput

if __name__ == '__main__':
    array = []
    zzz = writeCSV("geo_data.csv")
    parser = argparse.ArgumentParser(
        description='Log-Bilinear model for relation extraction.')
    _arg = parser.add_argument
    _arg('--read-dump', type=str, action='store',
         metavar='PATH', help='Reads in a wikidata json dump.')
    args = parser.parse_args()
    
    train_set = None
    if args.read_dump or True:
        dump_in = BZ2File(args.read_dump, 'r')
        #dump_in = open("latest-all.json","rb")
        line = dump_in.readline();
        iter = 0
        counter =0
        temp_array = []
        while line != '':
            iter += 1
            counter = counter + 1
            if counter%100000==0:
                print(counter)
                results = pool.map(to_triplets, temp_array)
                for record in results:
                    if record:
                        array.append(["|".join(record.get("label",[])),"|".join(record.get("alias",[])),
                                                                        "|".join(record.get("parent",[])),
                                                                        "|".join(record.get("population",[])),
                                                                        "|".join(record.get("instance",[])),
                                                                        "|".join(record.get("continent",[])),
                                                                        "|".join(record.get("country",[])),
                                                                        "|".join(record.get("capital",[])),
                                                                        "|".join(record.get("area",[])),
                                                                        "|".join(record.get("ent",[]))])
                
                
                
                
                temp_array = []
            line = dump_in.readline()
            flag= 1
            try:
                temp_array.append(line)
            except:
                pass
            if iter % 1000 == 0:
                sys.stdout.flush()
        
        results = pool.map(to_triplets, temp_array)
        for record in results:
            if record:
                array.append(["|".join(record.get("label",[])),"|".join(record.get("alias",[])),
                                                                        "|".join(record.get("parent",[])),
                                                                        "|".join(record.get("population",[])),
                                                                        "|".join(record.get("instance",[])),
                                                                        "|".join(record.get("continent",[])),
                                                                        "|".join(record.get("country",[])),
                                                                        "|".join(record.get("capital",[])),
                                                                        "|".join(record.get("area",[])),
                                                                        "|".join(record.get("ent",[]))])
        
        
        for a in array:
            label_dict[a[-1]] = a[0]
        for a in array:
            a[2] = "|".join([label_dict.get(entry,a[2]) for entry in a[2].split("|")])
            a[4] = "|".join([label_dict.get(entry,a[4]) for entry in a[4].split("|")])
            a[5] = "|".join([label_dict.get(entry,a[5]) for entry in a[5].split("|")])
            a[6] = "|".join([label_dict.get(entry,a[6]) for entry in a[6].split("|")])
            a[7] = "|".join([label_dict.get(entry,a[7]) for entry in a[7].split("|")])
            zzz.writerow(a)