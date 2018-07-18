from cassandra.cluster import Cluster
import pandas as pd

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

cluster = Cluster(contact_points=['ec2-54-85-62-208.compute-1.amazonaws.com'])

session = cluster.connect()
session.set_keyspace('clickstream')
session.row_factory = pandas_factory
session.default_fetch_size = 10000000 #needed for large queries, otherwise driver will do pagination. Default is 50000.

rows = session.execute("""select * from click_count_by_country_city""")
df = rows._current_rows
print df.head()
