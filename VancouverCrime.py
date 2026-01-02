import pandas
import numpy
from datetime import datetime
from utm import to_latlon
from multiprocessing import Pool, cpu_count

# The columns of the data sets.
columns = ['TYPE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'NEIGHBOURHOOD', 'X', 'Y']

# We'll change the hour column into a time of day variable that indicates whether the crime occurred at morning,
# afternoon, evening or night. And we'll convert the utm coordinates into latitudes and longitudes for tableau to use.
variables = ['TYPE', 'YEAR', 'DATE', 'TIMEOFDAY', 'NEIGHBOURHOOD', 'LATITUDE', 'LONGITUDE']


# The data source separated their data into a csv for each year. So we'll want to put them all together into one csv.
# This function will return the dataframe we want for a given year.
def process_function(year):
    global columns, variables
    data = pandas.DataFrame(columns=variables)

    # Get the data for the year.
    path = r"C:\Users\tbaka\OneDrive\Documents\data\Vancouver Crime\individual years\crimedata_csv_AllNeighbourhoods_" \
           + year + '.csv'
    year_data = pandas.read_csv(path)
    year_data = year_data[columns]

    n = len(year_data)
    # This will iterate through all the data, put it into the form we want and add it to our empty dataframe.
    for j in range(n):
        row = year_data.iloc[j]

        # Converts the utm coordinates to latitudes and longitudes
        x = row.X
        y = row.Y
        if (100000 < x < 999999) and (0 < y < 10000000):
            # Vancouver's utm zone is 10 North.
            latitude, longitude = to_latlon(row.X, row.Y, 10, northern=True)
        else:
            # If to_latlan can't convert x and y, then set latitude and longitude to be null.
            latitude = longitude = numpy.nan

        date = datetime(row.YEAR, row.MONTH, row.DAY)

        # Determines what part of the day the crime occurred at. I chose noon to 5pm for afternoon and 5pm to 10pm
        # for evening.
        if 6 <= row.HOUR <= 12:
            time_of_day = 'Morning'
        elif 12 < row.HOUR <= 17:
            time_of_day = 'Afternoon'
        elif 17 < row.HOUR <= 22:
            time_of_day = 'Evening'
        else:
            time_of_day = 'Night'

        row = [row.TYPE, row.YEAR, date, time_of_day, row.NEIGHBOURHOOD, latitude, longitude]
        data.loc[j] = row
    return data


if __name__ == '__main__':
    start_year = 2015
    end_year = 2025
    years = numpy.arange(start_year, end_year + 1)
    years = numpy.astype(years, str)

    # multiprocessing for speeding up the data transformation.
    number_of_cpus = min(5, cpu_count())
    with Pool(number_of_cpus) as p:
        results = p.map(process_function, years)

    # Puts all the data together into one data frame and then saves it as a csv.
    crime_data = pandas.concat(results)
    crime_data = crime_data.reset_index(drop=True)
    crime_data.to_csv(r"C:\Users\tbaka\OneDrive\Documents\data\Vancouver Crime\crimedata.csv")
