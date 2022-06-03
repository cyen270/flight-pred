from flask import Flask, render_template, request, url_for
from predict import estimate_delay, airline_otp, airport_otp

app = Flask(__name__)

# @app.route('/')
# def serve_home():
#     return render_template('index.html')

# Handle form post requests
@app.route('/', methods=['GET', 'POST'])
def main():
    if request.method == 'POST':
        user_inputs = request.form
        flight_number = user_inputs['flight-number']
        departure_date = user_inputs['departure-date']

        delay_info = estimate_delay(flight_number, departure_date)

        # If flight is not found
        if delay_info is None:

            error_msg = 'The provided flight was not found.  Please check your inputs and try again.'
            return render_template('flight_not_found.html',
                        msg=error_msg)

        else:
            # Could pass params as a dictionary instead
            return render_template('results.html', 
                        flight_number=flight_number,
                        departure_date=departure_date,
                        departure_time=delay_info['DEPART_TIME'],
                        origin_airport=delay_info['ORIGIN'],
                        dest_airport=delay_info['DESTINATION'],
                        lo_delay=delay_info['LO_DELAY'],
                        hi_delay=delay_info['HI_DELAY'])

    else:
        return render_template('index.html')

@app.route('/airlines', methods=['GET'])
def airline_summary():
    otp = airline_otp()
    return render_template('airlines.html', 
                        otp=otp)

@app.route('/airports', methods=['GET'])
def airport_summary():
    otp = airport_otp()
    return render_template('airports.html',
                        otp=otp)

if __name__ == '__main__':
    # For local testing
    app.run(host='0.0.0.0', port=8080, debug=True)
