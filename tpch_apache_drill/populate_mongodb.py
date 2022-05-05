import os
import time
import glob

table_names = [x.split('/')[-1][:-5] for x in glob.glob('./jsons/*.json')]

tick_f = time.time()

for table_name in table_names:
    tick = time.time()

    os.system(
        f"""mongoimport --jsonArray --db tpch --collection {table_name} --file jsons/{table_name}.json""")

    tock = time.time()
    print("Time taken to populate %s table: %.2f s" %
          (table_name, tock - tick))

tock = time.time()

print("Total time taken to populate: %.2f s" % (tock - tick_f))
