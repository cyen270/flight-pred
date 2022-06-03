''' Ingest flights and aircraft registration data from websites '''

from urllib.request import urlopen
import code
import os
import zipfile
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import re

def dl_aircraft_reg(year, destdir):
    ''' Download aircraft registration data from the FAA website
        Specify the year for which the data should be pulled (2012+) '''
    BASE_URL = "https://registry.faa.gov/database/yearly/ReleasableAircraft."
    url = BASE_URL + "{}.zip".format(year)
    response = urlopen(url)
    dest_filepath = os.path.join(destdir, "{}.zip".format(year))
    with open(dest_filepath, 'wb') as f:
        f.write(response.read())
    return(dest_filepath)

def unzip_aircraft_reg(filename, destdir):
    ''' Unzip file to a specified directory.  
        Specify the path of the zip file. '''
    
    zipf = zipfile.ZipFile(filename, 'r')
    cwd = os.getcwd()
    os.chdir(destdir)
    zipf.extractall()
    zipf.close()
    os.chdir(cwd)

    filelist = []
    pattern = re.compile('.*\.txt')
    txtfiles = list(filter(pattern.match, zipf.namelist()))
    basedir = os.path.dirname(filename)
    
    os.system("sed -i '188726d' /home/calvinyen100/flight-pred/data/DEREG.txt")

    for f in txtfiles:
        f = os.path.join(basedir, f)
        print(f)
        df = pd.read_csv(f, sep=',', index_col=[0], low_memory=False)
        filename = os.path.basename(f)
        filename_noext = os.path.splitext(f)[0]
        print('Read {} rows'.format(len(df)))
        pqfilename = filename_noext+'.parquet'

        df[df.dtypes[df.dtypes=='object'].index] = \
            df[df.dtypes[df.dtypes=='object'].index].astype('string')

        df.to_parquet(pqfilename)
        os.system('rm {}'.format(f))
        filelist.append(os.path.join(basedir, pqfilename))

    return filelist

def upload_aircraft_reg(bucketname, filepaths):
    ''' Upload files to Cloud Storage under the
        specified bucket '''
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)
    for path in filepaths:
        blob = bucket.blob(os.path.basename(path))
        blob.upload_from_filename(path)
 
    return None

def load_bq_aircraft_reg(bucketname, file_list):
    ''' Load aircraft registration data into BigQuery '''

    # Initialize BigQuery and Cloud Storage clients for API calls
    bq_client = bigquery.Client()
    storage_client = storage.Client()

    # BQ job configuration - configure to use Parquet format
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('IATA_CODE', 'STRING'),
            bigquery.SchemaField('NAME', 'STRING')
        ],
        skip_leading_rows=1
    )
    
    # Create regular expression to match the list of files to load to BQ
    file_list = '|'.join(file_list)

    for blob in storage_client.list_blobs(bucketname):
        # If blob name matches a file in the provided file list, load to BQ
        if re.search(file_list, blob.name) is not None:
            file_name = os.path.splitext(blob.name)[0]
            uri = "gs://flight-pred-347402.appspot.com/airlines.csv"
            # uri = os.path.join('gs://', blob.bucket.name, blob.name)
            print(uri)
            table_id = "flight-pred-347402.master.test_airlines"

            load_job = bq_client.load_table_from_uri(
                source_uris=uri, 
                destination=table_id,
                job_config=job_config
            )

            load_job.result()

            result_table = bq_client.get_table(table_id)
            print('Loaded {} rows to table {}'.format(result_table.num_rows, table_id))
        
        else:
            pass

    return None



def ingest(year, dirname, bucket, file_list):
    zipfiles = dl_aircraft_reg(year, dirname)
    unzipped_files = unzip_aircraft_reg(zipfiles, dirname)
    upload_aircraft_reg(bucket, unzipped_files)
    # load_bq_aircraft_reg(bucket, file_list)
    print('Successfully ingested files to Cloud Storage...')


if (__name__ == '__main__'):
    year = 2021
    dirname = '/home/calvinyen100/flight-pred/data'
    bucket = 'flight-pred-347402.appspot.com'
    file_list = ['ENGINE', 'ACFTREF']
    ingest(year, dirname, bucket, file_list)


