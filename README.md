# AQI-seeker
I am trying to find a better day for running in the next several days.

AQI-seeker is going to predict the AQI of Shanghai, China. The haze is so terrible that people have to adapt
my life plan to avoid outdoor in the haze day. At present, I don't found an available tool to
predict the haze. It is why I create this project.

## Overview
  * First, AQI-seeker used the history weather data of Shanghai and city around Shanghai to train a model. I am
  going to use the Spark(MLlib) to train the model, and first I try the Random Forest algorithm.
  * Second use this model to predict AQI in the next several days.
  * At last, I will find the "best" algorithm according to the accuracy.
  Weather data will including the weather station with the distance from Shanghai:
    - Less than 100 km
    - Around 300km
    - Around 600km
    - Around 1000km

  Weather data will items including:
  - Temperature
  - Humidity
  - Wind
  - Precipitation
  - Others to find

## Step
 1. Get history weather data from U.S.embassy and http://gis.ncdc.noaa.gov/.
 2. Pre-process the data and get the data item interesting
 3. Compute the Pearson's correlation between AQI and different weather items
 4. Train the model with the strong relation items, using Random Forest algorithm
 5. Predict AQI
 6. Train the model using different algorithm, and find the "best" algorithm.


## Contributor
  Anyone interested in the project is welcome!