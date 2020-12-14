# method of directly working with Impala and other distributed execution engines

import ibis

# connect to Impala
IMPALA_HOST = os.getenv('IMPALA_HOST', '10.0.1.34')
con = ibis.impala.connect(host=IMPALA_HOST, port=21050, database='default')

# EXPLORE
# list tables in database
con.list_tables()

# select table
all = con.table('telco_churn')

# display metadata
all.schema()
all.metadata()

# display selected data
print(all['customerid', 'churn'])
all['customerid', 'churn'].execute()
print(all[all.churn == 'Yes'])
all[all.churn == 'Yes'].execute()
      
# calculate average tenure by different contact type, limited to check payments only
all \
  .filter(all.paymentmethod.like(['%check%'])) \
  .group_by('contract') \
  .aggregate( \
     avg_tenure=all.tenure.mean() \
  ) \
  .sort_by(ibis.desc('avg_tenure')) \
  .execute()
  
# read into dataframe
import pandas as pd

# store content into panda
delta = all[all]

# load serialized model using joblib/pickle/...
# loaded_model = joblib.load(filename)
# loaded_model = pickle.load(open(filename, 'rb'))

-----

# predict / score data
# loaded_model.predict(delta)

# add churn column (to prefabricate model's prediction) 
mutated = delta.mutate(churn="Yes")
delta = mutated['customerid', 'churn']

# print(ibis.impala.compile(delta))

# create table from panda
con.create_table('scored_delta', delta)

# confirm that data has been loaded properly
delta_impala = con.table('scored_delta')
print(ibis.impala.compile(delta_impala[delta_impala]))
delta_impala.execute()

