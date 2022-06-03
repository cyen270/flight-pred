from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery

from tempfile import TemporaryFile

import pandas as pd
import numpy as np

import joblib
import requests
import re

# GCP project information
PROJECT_ID = 'flight-pred-347402'

# GCP cloud storage, model, and encoder information
BUCKET_NAME = 'flight-pred-347402.appspot.com'
MODEL_PATH = 'models/linreg_predictor.joblib'
ORD_ENC_PATH = 'models/label_encoder.joblib'
ZSCORE_ENC_PATH = 'models/std_scaler.joblib'

# Secret information
SECRET_NAME = 'flightlabs_api'
SECRET_VERSION = 1
FULL_SECRET_NAME = f'projects/70192230239/secrets/{SECRET_NAME}/versions/{SECRET_VERSION}'

storage_client = storage.Client(PROJECT_ID)
bq_client = bigquery.Client(PROJECT_ID)

# FlightLabs API
API_URL = 'https://app.goflightlabs.com/flights'

# Load stored job files
def load_file(filepath):
    '''
    Load the default model upon application startup
    '''

    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(filepath)

    with TemporaryFile() as temp_file:
        blob.download_to_file(temp_file)
        temp_file.seek(0)
        f = joblib.load(temp_file)

    return f

def flight_details(flight_number):
    '''
    Given a flight number (flight_number), call the
    FlightLabs API and return a dictionary containing
    all available information on that flight
    '''
    
    # Retrieve FlightLabs API key
    secrets_client = secretmanager.SecretManagerServiceClient()
    secret_response = secrets_client.access_secret_version(request={'name': FULL_SECRET_NAME})
    
    # Retrieve flight information for the provided flight_number
    api_key = secret_response.payload.data.decode('UTF-8')
    
    params = {'access_key': api_key, 
            'flight_iata': flight_number}

    response = requests.get(API_URL, params)

    try:
        flight_info = response.json()['0']
    except:
        try:
            flight_info = response.json()[0]
        except:
            return None

    return flight_info

def extract_model_params(flight_number, departure_date):
    '''
    Prepare the inputted data for model consumption. 
    '''

    flight_info = flight_details(flight_number)
    if flight_info is None:
        return None

    # Extract airline and airport information
    airline = flight_info['airline']['iata']
    origin_airport = flight_info['departure']['iata']
    dest_airport = flight_info['arrival']['iata']

    # Extract departure time information
    departure_time = flight_info['departure']['scheduled']

    r = re.compile('.*T([0-2][0-9]):([0-5][0-9]).*')
    match = re.search(r, departure_time)

    depart_hour = match.group(1)
    depart_min = match.group(2)

    # Extract historical delay information 
    query = '''
        SELECT AIRLINE, ORIGIN_AIRPORT, DESTINATION_AIRPORT, HIST_DEPART_DELAY
        FROM `master.departure_delays`
        WHERE AIRLINE = '{0}'
        AND ORIGIN_AIRPORT = '{1}'
        AND DESTINATION_AIRPORT = '{2}'
        LIMIT 1
        '''.format(airline, origin_airport, dest_airport)

    query_results = bq_client.query(query)

    hist_delay = 0

    for row in query_results:
        hist_delay = row['HIST_DEPART_DELAY']


    # Prepare data for model consumption
    model_inputs = {'AIRLINE': airline,
                    'ORIGIN_AIRPORT': origin_airport,
                    'DESTINATION_AIRPORT': dest_airport,
                    'depart_hour': depart_hour,
                    'depart_min': depart_min,
                    'HIST_DEPARTURE_DELAY': hist_delay}

    return model_inputs

def estimate_delay(flight_number, departure_date):
    '''
    Given a model, flight number, and flight departure date,
    provide an estimate of that flight's departure delay
    '''

    result_dict = {}

    # Extract flights data 
    model_inputs = extract_model_params(flight_number, departure_date)
    if model_inputs is None:
        return None
        
    inputs_df = pd.DataFrame(model_inputs, index=[1])
    
    # Transform data for model consumption 
    categ_vars = ['AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']
    num_vars = ['depart_hour', 'depart_min', 'HIST_DEPARTURE_DELAY']

    cat_data = ord_enc.transform(inputs_df[categ_vars])
    num_data = zscore_enc.transform(inputs_df[num_vars])

    input_data = np.concatenate([cat_data, num_data], axis=1)

    # Obtain delay prediction
    est_delay = model.predict(input_data)
    est_delay = round(est_delay[0], 0)

    uncertainty = 15

    lo_delay = max(0, est_delay - uncertainty)
    hi_delay = est_delay + uncertainty

    # Summarize predicted results
    result_dict['AIRLINE'] = model_inputs['AIRLINE']
    result_dict['ORIGIN'] = model_inputs['ORIGIN_AIRPORT']
    result_dict['DESTINATION'] = model_inputs['DESTINATION_AIRPORT']
    result_dict['DEPART_TIME'] = '{0}:{1}'.format(
        model_inputs['depart_hour'], model_inputs['depart_min']
    )
    result_dict['LO_DELAY'] = lo_delay
    result_dict['HI_DELAY'] = hi_delay

    return result_dict

def airline_otp():
    on_time_perf = bq_client.query('''
        select airline_name as airline, 
            sum(if(ARRIVAL_DELAY > 0, 1, 0)) / count(*) as percent_delayed,
            avg(ARRIVAL_DELAY) as avg_delay,
            sum(if(CANCELLED > 0, 1, 0)) / count(*) as cancellation_rate, 
            count(*) as flights_serviced
        from `master.combined_data`
        group by airline_name
        order by percent_delayed desc
        ''').to_dataframe()

    on_time_perf['percent_delayed'] = on_time_perf['percent_delayed'].map(lambda x: '{}%'.format(round(x*100, 1)))
    on_time_perf['avg_delay'] = on_time_perf['avg_delay'].round(1)
    on_time_perf['cancellation_rate'] = on_time_perf['cancellation_rate'].map(lambda x: '{}%'.format(round(x*100, 1)))
    on_time_perf['flights_serviced'] = on_time_perf['flights_serviced'].map(lambda x: '{:,.0f}'.format(x))

    on_time_perf.columns = ['Airline', 
                        'Delay Rate', 
                        'Average Delay (minutes)', 
                        'Cancellation Rate',
                        'Annual Flights Serviced']

    on_time_perf = on_time_perf.to_html(index=False, 
                                    justify='center', 
                                    border=0,
                                    classes=['table', 'table-striped', 'table-hover', 'table-sm', 'text-center'])

    return on_time_perf


def airport_otp():
    origin_airport_delay = bq_client.query('''
        select ORIGIN_AIRPORT,
            origin_airport_name, 
            sum(if(ARRIVAL_DELAY > 0, 1, 0)) / count(*) as percent_delayed,
            avg(ARRIVAL_DELAY) as avg_delay,
            sum(if(CANCELLED > 0, 1, 0)) / count(*) as cancellation_rate,
            count(*) as flights_serviced
        from `master.combined_data`
        group by ORIGIN_AIRPORT, origin_airport_name
        having flights_serviced > 10000
        order by percent_delayed desc
        limit 20
        ''').to_dataframe()

    origin_airport_delay['percent_delayed'] = origin_airport_delay['percent_delayed'].map(lambda x: '{}%'.format(round(x*100, 1)))
    origin_airport_delay['avg_delay'] = origin_airport_delay['avg_delay'].round(1)
    origin_airport_delay['cancellation_rate'] = origin_airport_delay['cancellation_rate'].map(lambda x: '{}%'.format(round(x*100, 1)))
    origin_airport_delay['flights_serviced'] = origin_airport_delay['flights_serviced'].map(lambda x: '{:,.0f}'.format(x))

    origin_airport_delay.columns = ['Airport Code',
                                'Airport Name',
                                'Delay Rate',
                                'Average Delay (minutes)',
                                'Cancellation Rate',
                                'Annual Flights Serviced']

    origin_airport_delay = origin_airport_delay.to_html(index=False, 
                                    justify='center', 
                                    border=0,
                                    classes=['table', 'table-striped', 'table-hover', 'table-sm', 'text-center'])

    return origin_airport_delay
            

model = load_file(MODEL_PATH)
ord_enc = load_file(ORD_ENC_PATH)
zscore_enc = load_file(ZSCORE_ENC_PATH)






