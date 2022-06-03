# [Flying with Pongo](https://flight-pred-347402.uc.r.appspot.com/)
*Flight Delay Predictor*

Web application that predicts the departure delay for a given flight and departure date.  Example usage below:

<img src='https://github.com/cyen270/flight-pred/blob/main/Pongo-Flight-Pred-Example.png' alt='Pongo Example' width='540'>


## Technology Stack
- Cloud services: The application is built using Google Cloud services (e.g., App Engine, Cloud Storage, BigQuery).  
- Web frameworks: The Flask framework is used to handle requests. 
- ML modeling: The model used to serve predictions is an ensemble of random forest and linear regression estimators. 
- APIs: The application calls FlightLabs API services to retrieve flight information. 


## CI Build Status
[![cyen270](https://circleci.com/gh/cyen270/flight-pred.svg?style=svg)](https://circleci.com/gh/cyen270/flight-pred)
