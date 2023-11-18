import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Math;

public class MeteoReduce extends Reducer<Text, MeteoWritable, Text, MeteoWritable> {
    private MeteoWritable result = new MeteoWritable();

    @Override
    protected void reduce(Text key, Iterable<MeteoWritable> values, Context context) throws IOException, InterruptedException {
        int elevationCount = 0;
        int windDirectionCount = 0;
        int ceilingHeightCount = 0;
        int distanceCount = 0;
        int temperatureCount = 0;
        int pressureCount = 0;
        int windSpeedCount = 0;
        int airTempCount = 0;

        double elevationSum = 0.0;
        double windDirectionSum = 0.0;
        double ceilingHeightSum = 0.0;
        double distanceSum = 0.0;
        double temperatureSum = 0.0;
        double pressureSum = 0.0;
        double windSpeedSum = 0.0;
        double airTempSum = 0.0;
        
        double elevationMin = Double.MAX_VALUE;
        double windDirectionMin = Double.MAX_VALUE;
        double ceilingHeightMin = Double.MAX_VALUE;
        double distanceMin = Double.MAX_VALUE;
        double temperatureMin = Double.MAX_VALUE;
        double pressureMin = Double.MAX_VALUE;
        double windSpeedMin = Double.MAX_VALUE;
        double airTempMin = Double.MAX_VALUE;

        double elevationMax = Double.MIN_VALUE;
        double windDirectionMax = Double.MIN_VALUE;
        double ceilingHeightMax = Double.MIN_VALUE;
        double distanceMax = Double.MIN_VALUE;
        double temperatureMax = Double.MIN_VALUE;
        double pressureMax = Double.MIN_VALUE;
        double windSpeedMax = Double.MIN_VALUE;
        double airTempMax = Double.MIN_VALUE;
        

        for (MeteoWritable val : values) {
            // Calculate sum and update min/max values for each variable
            if (!Double.isNaN(val.getElevation())) {
                elevationSum += val.getElevation();
                elevationCount++;
                elevationMin = Math.min(elevationMin, val.getElevation());
                elevationMax = Math.max(elevationMax, val.getElevation());
            }

            if (!Double.isNaN(val.getWindDirection())) {
                windDirectionSum += val.getWindDirection();
                windDirectionCount++;
                windDirectionMin = Math.min(windDirectionMin, val.getWindDirection());
                windDirectionMax = Math.max(windDirectionMax, val.getWindDirection());
            }

            if (!Double.isNaN(val.getCeilingHeight())) {
                ceilingHeightSum += val.getCeilingHeight();
                ceilingHeightCount++;
                ceilingHeightMin = Math.min(ceilingHeightMin, val.getCeilingHeight());
                ceilingHeightMax = Math.max(ceilingHeightMax, val.getCeilingHeight());
            }

            if (!Double.isNaN(val.getDistance())) {
                distanceSum += val.getDistance();
                distanceCount++;
                distanceMin = Math.min(distanceMin, val.getDistance());
                distanceMax = Math.max(distanceMax, val.getDistance());
            }

            if (!Double.isNaN(val.getTemperature())) {
                temperatureSum += val.getTemperature();
                temperatureCount++;
                temperatureMin = Math.min(temperatureMin, val.getTemperature());
                temperatureMax = Math.max(temperatureMax, val.getTemperature());
            }

            if (!Double.isNaN(val.getPressure())) {
                pressureSum += val.getPressure();
                pressureCount++;
                pressureMin = Math.min(pressureMin, val.getPressure());
                pressureMax = Math.max(pressureMax, val.getPressure());
            }

            if (!Double.isNaN(val.getWindSpeed())) {
                windSpeedSum += val.getWindSpeed();
                windSpeedCount++;
                windSpeedMin = Math.min(windSpeedMin, val.getWindSpeed());
                windSpeedMax = Math.max(windSpeedMax, val.getWindSpeed());
            }

            if (!Double.isNaN(val.getAirTemp())) {
                airTempSum += val.getAirTemp();
                airTempCount++;
                airTempMin = Math.min(airTempMin, val.getAirTemp());
                airTempMax = Math.max(airTempMax, val.getAirTemp());
            }
        }

        // Calculate averages for each variable
        result.setElevation(elevationCount == 0 ? Double.NaN : elevationSum / elevationCount);
        result.setWindDirection(windDirectionCount == 0 ? Double.NaN : windDirectionSum / windDirectionCount);
        result.setCeilingHeight(ceilingHeightCount == 0 ? Double.NaN : ceilingHeightSum / ceilingHeightCount);
        result.setDistance(distanceCount == 0 ? Double.NaN : distanceSum / distanceCount);
        result.setTemperature(temperatureCount == 0 ? Double.NaN : temperatureSum / temperatureCount);
        result.setPressure(pressureCount == 0 ? Double.NaN : pressureSum / pressureCount);
        result.setWindSpeed(windSpeedCount == 0 ? Double.NaN : windSpeedSum / windSpeedCount);
        result.setAirTemp(airTempCount == 0 ? Double.NaN : airTempSum / airTempCount);

        // Set min and max values for each variable
        result.setElevationMin(elevationMin);
        result.setElevationMax(elevationMax);

        result.setWindDirectionMin(windDirectionMin);
        result.setWindDirectionMax(windDirectionMax);

        result.setCeilingHeightMin(ceilingHeightMin);
        result.setCeilingHeightMax(ceilingHeightMax);

        result.setDistanceMin(distanceMin);
        result.setDistanceMax(distanceMax);

        result.setTemperatureMin(temperatureMin);
        result.setTemperatureMax(temperatureMax);

        result.setPressureMin(pressureMin);
        result.setPressureMax(pressureMax);

        result.setWindSpeedMin(windSpeedMin);
        result.setWindSpeedMax(windSpeedMax);

        result.setAirTempMin(airTempMin);
        result.setAirTempMax(airTempMax);


        context.write(key, result);
    }
}
