# Big Data Weather Analysis Project

This project focuses on collecting, processing, and visualizing weather data from the National Oceanic and Atmospheric Administration (NOAA). The project utilizes Python for data manipulation and analysis, Hadoop for distributed data processing, and MapReduce for distributed data processing.

## Project Components

### Data Collection

- **NOAA NCEI Data Access:** [https://www1.ncdc.noaa.gov/](https://www1.ncdc.noaa.gov/)
- Use `download_data.py` to scrape the data from the website.
- Use `combineFiles.py` to collect all the text files into one file.

### Data Processing

#### Hadoop

Ensure you have Hadoop properly installed and configured. All the required JAR files have been uploaded to run the JAR file in Hadoop.

1. Place the `combinedfile` in HDFS.
3. Use MapReduce to process and aggregate the data run the jar file `MonthlyWeatherData.jar` on the combined data .
     - Java is used for the mapreduce
     - Calculate monthly average temperature (daytime and nighttime), precipitation depth, pressure, and wind speed.
     - Find monthly minimum and maximum temperatures, precipitation depth, pressure, and wind speed.
4. Copy the output of the MapReduce to the local.

### Data Visualization

- Power BI is employed to create interactive dashboards for visualizing the weather data.
![Power BI Dashboard](Dashboard.png)
