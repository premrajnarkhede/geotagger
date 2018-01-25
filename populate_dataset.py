import dataset
import re
from unidecode import unidecode

def removeAllPunctuations(g):
    g= g.replace(".","")
    g= g.replace(",","")
    g= g.replace("'","")
    g= g.replace("-"," ")
    g= g.replace("/","")
    g= g.replace(":","")
    g= g.replace(";","")
    g= g.replace(".","")
    g= g.replace('"',"")
    g= g.replace("*","")
    g= g.replace("["," ")
    g= g.replace("]"," ")
    g= g.replace("("," ")
    g= g.replace(")"," ")
    g= g.replace("<"," ")
    g= g.replace(">"," ")
    g= g.replace("="," ")
    g= g.replace(","," ")
    g= re.sub( '\s+', ' ', g ).strip()
    return g

def readCSV(path):
    import unicodecsv
    reader = unicodecsv.reader(open(path,'rU'), delimiter=',', quotechar='"')
    return reader
db = dataset.connect('sqlite:///locations.db')
table = db["main"]
table.drop()
table = db["main"]
reader = readCSV("geo_data.csv")
for rindex,row in enumerate(reader):
    try:
        main, alias, blank, population, wikidatainstance, continent, country, capitalof, area, wikiid = row
    except Exception,e:
        print e
        print "Error in parsing columns"
    if population!="":
        try:
            population_tuples = population.split("|")
            max_value = 0
            for entry in population_tuples:
                entry_dict = eval(entry)
                amount = int(entry_dict["datavalue"]["value"]["amount"].replace("+","").replace(".",""))
                if amount>max_value:
                    max_value = amount 
            population = max_value
        except Exception,e:
            print e
            population = 0
            print "Error in parsing population"
            print main
    else:
        population = 0
    
    alias = alias.split("|")
    alias.append(main)
    alias = set([removeAllPunctuations(a.lower()) for a in alias])
    for al in alias:
        table.insert(dict(main=main, alias=al, population=population, wikidatainstance=wikidatainstance, continent=continent
                          , country=country, capitalof=capitalof, wikiid=wikiid))
    
    if rindex%1000==0:
        db.commit()
        print rindex,"Number of rows processed"
        
    table.create_index(['alias'])