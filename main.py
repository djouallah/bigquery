
import os
import pandas as pd
import gcsfs
from google.cloud import storage
from google.cloud import bigquery
import re ,shutil
from urllib.request import urlopen
curDir = os.getcwd()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =(curDir +'\\'+'test-990c2f64d86d.json')

filelist_storage=[]

        
client = storage.Client()
BUCKET_NAME = 'aemo_bq'
bucket = client.get_bucket(BUCKET_NAME)

blobs = bucket.list_blobs()

for blob in blobs:
   filelist_storage.append(blob.name)
current = [w.replace('.CSV', '.zip') for w in filelist_storage]
url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
result = urlopen(url).read().decode('utf-8')
pattern = re.compile(r'[\w.]*.zip')
filelist = pattern.findall(result)



files_to_upload = list(set(filelist) - set(current))
client = bigquery.Client()

dataset_ref = client.dataset('aemo')
table_ref = dataset_ref.table('daily')
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.schema = [
   bigquery.SchemaField("SETTLEMENTDATE", "TIMESTAMP"),
   bigquery.SchemaField("DUID", "STRING"),
   bigquery.SchemaField("SCADAVALUE", "NUMERIC"),
                      ]


for x in files_to_upload:
   with urlopen(url+x) as source, open(x, 'w+b') as target:
    shutil.copyfileobj(source, target)
   zf = (curDir + '\\'+x)
   df = pd.read_csv(zf,skiprows=1,usecols=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],parse_dates=["SETTLEMENTDATE"])
   df=df.dropna(how='all') #drop na
   y = x.replace('.zip', '.CSV')
   df.to_csv(y,index=False,float_format="%.4f",date_format='%Y-%m-%dT%H:%M:%S.%fZ')
   storage_client = storage.Client()
   buckets = list(storage_client.list_buckets())
   bucket = storage_client.get_bucket(BUCKET_NAME)
   blob = bucket.blob(y)
   blob.upload_from_filename((curDir + '\\'+y))
   with open((curDir + '\\'+y), "rb") as source_file:
     job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

   job.result()  # Waits for table load to complete.
   print("Loaded {} rows into {}:{}.".format(job.output_rows, 'aemo', 'daily'))
   os.remove(os.path.join(curDir,x))
   os.remove(os.path.join(curDir,y))
