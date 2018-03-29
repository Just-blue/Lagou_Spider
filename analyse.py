from pymongo import MongoClient

import pandas as pd


def connect_mongo(host, port, db_name ,username=None, password=None):

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db_name)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db_name]

def read_mongo(db,collection,query={},no_id=True):

    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    return df

def sala_cut(salary):
    if "-" in salary:
        salary = salary.replace("k", "").replace("K", "")
    else:
        salary = salary.split("k")[0]
    try:
        min_s = salary.split("-")[0]
        max_s = salary.split("-")[1]
        return ((int(min_s) + int(max_s)) * 1000) // 2
    except IndexError:
        return int(salary) * 1000

if __name__ == '__main__':
    db = connect_mongo(host='localhost',port=27017,db_name='Lagou')

    df = read_mongo(db,'lagou')
    df_dup = df.drop_duplicates(subset='ID', keep='first')
    df_dup['avgSALARY']=df_dup.SALARY.apply(sala_cut)
    df_clean = df_dup[
            ["NAME", "ADDRESS", "ID", 'COMPANY', "avgSALARY", "INDUSTRY", "EXPERIENCE", "EDUCATION", "EDUCATION", "NATURE",
            "DETAIL"]
        ]

