import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeteoMap extends Mapper<LongWritable, Text, Text, MeteoWritable> {
    private Text year = new Text();
    private MeteoWritable meteoData = new MeteoWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Check the length of the line before extracting substrings
        if (line.length() >= 105) {
        	String yearValue = line.substring(15, 19);
        	//String mounthValue=line.substring(19, 21) ;
        	double elevation = line.substring(46, 51).equals("+9999") ? Double.NaN : Double.parseDouble(line.substring(46, 51)) ;
        	double windDirection = line.substring(60, 63).equals("999") ? Double.NaN : Double.parseDouble(line.substring(60, 63));
        	double ceilingHeight = line.substring(70, 75).equals("99999") ? Double.NaN : Double.parseDouble(line.substring(70, 75));
        	double distance = line.substring(78, 84).equals("999999") ? Double.NaN : Double.parseDouble(line.substring(78, 84)) ;
        	double temperature = line.substring(87, 92).equals("+9999") ? Double.NaN : Double.parseDouble(line.substring(87, 92)) / 10.0;
        	double pressure = line.substring(99, 104).equals("99999") ? Double.NaN : Double.parseDouble(line.substring(99, 104)) / 10.0;
        	double windSpeed = line.substring(66, 69).equals("999") ? Double.NaN : Double.parseDouble(line.substring(66, 69)) / 10.0;
        	double airTemp = line.substring(87, 92).equals("+9999") ? Double.NaN : Double.parseDouble(line.substring(87, 92)) / 10.0;


            meteoData.setElevation(elevation);
            meteoData.setWindDirection(windDirection);
            meteoData.setCeilingHeight(ceilingHeight);
            meteoData.setDistance(distance);
            meteoData.setTemperature(temperature);
            meteoData.setPressure(pressure);
            meteoData.setWindSpeed(windSpeed);
            meteoData.setAirTemp(airTemp);

            year.set(yearValue);
            context.write(year, meteoData);
        }
    }
}
