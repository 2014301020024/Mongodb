
import time
import datetime
import pymongo
import pandas as pd
from pymatgen import Structure
from pymatgen.analysis.structure_matcher import StructureMatcher
from pymongo import MongoClient

# ---------------------------------------------------------------------------------------------------------------------
link = pymongo.MongoClient('mongodb://xx:xx@xx/xx')
db = link.webdev

def data_excel(lst):
    link, bandgap, formula, plane, e_above_hull, pointgroup = [], [], [], [], [], []
    for i in lst:
        link.append('http://293l6o4188.qicp.vip:54031/materials/' + i['struct_id'])
        formula.append(i['formula'])
        pointgroup.append(i["pointgroup"])
        e_above_hull.append(i["E_above_hull"])
        bandgap.append(i["Bandgap"])
        plane.append(i['plane'])
    data = {}
    data['formula'] = formula
    data["pointgroup"] = pointgroup
    data["E_above_hull"] = e_above_hull
    data["Bandgap"] = bandgap
    data['plane'] = plane
    symbol = link
    df2 = pd.DataFrame(data, index=symbol)
    print(df2)
    with open('xx.xls', 'a') as file:
        file.close()
    df2.to_excel('xx.xls')

# ---------------------------------------------------------------------------------------------------------------------

with open('xx.txt', "r") as file:
    total = eval(file.read())
struct_ids = [i["struct_id"] for i in total]

insert = {'struct_id': '-', 'formula': '-', 'pointgroup': '-', 'E_above_hull': "-", 'Bandgap': "-",  'plane': "-"}

print(len(struct_ids))
series = []
index1 = []
count = 0
lst = []
for i, j in enumerate(struct_ids):
    formula = []
    if i not in index1:
        lst.append(total[i])
        index1.append(i)
        doc = db.materials.find_one({"struct_id": j})
        structure = Structure.from_dict(doc["relaxed_structure"]).get_primitive_structure()
        representation = doc["pretty_formula"]
        formula.append(representation)
        for k, l in enumerate(struct_ids):
            if k not in index1:
                doc_1 = db.materials.find_one({"struct_id": l})
                structure_1 = Structure.from_dict(doc_1["relaxed_structure"]).get_primitive_structure()
                if StructureMatcher().fit_anonymous(structure, structure_1) == True:
                    formula.append(doc_1["pretty_formula"])
                    lst.append(total[k])
                    index1.append(k)
        lst.append(insert)
        print(lst)
    data_excel(lst)
