import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class MeteoWritable implements Writable {
    private double elevation;
    private double windDirection;
    private double ceilingHeight;
    private double distance;
    private double temperature;
    private double pressure;
    private double windSpeed;
    private double airTemp;
    
    private double elevationMin;
    private double windDirectionMin;
    private double ceilingHeightMin;
    private double distanceMin;
    private double temperatureMin;
    private double pressureMin;
    private double windSpeedMin;
    private double airTempMin;

    private double elevationMax;
    private double windDirectionMax;
    private double ceilingHeightMax;
    private double distanceMax;
    private double temperatureMax;
    private double pressureMax;
    private double windSpeedMax;
    private double airTempMax;

    public MeteoWritable() {
        this.elevation = 0.0;
        this.windDirection = 0.0;
        this.ceilingHeight = 0.0;
        this.distance = 0.0;
        this.temperature = 0.0;
        this.pressure = 0.0;
        this.windSpeed = 0.0;
        this.airTemp = 0.0;

        // Initialize minimum values
        this.elevationMin = Double.MAX_VALUE;
        this.windDirectionMin = Double.MAX_VALUE;
        this.ceilingHeightMin = Double.MAX_VALUE;
        this.distanceMin = Double.MAX_VALUE;
        this.temperatureMin = Double.MAX_VALUE;
        this.pressureMin = Double.MAX_VALUE;
        this.windSpeedMin = Double.MAX_VALUE;
        this.airTempMin = Double.MAX_VALUE;

        // Initialize maximum values
        this.elevationMax = Double.MIN_VALUE;
        this.windDirectionMax = Double.MIN_VALUE;
        this.ceilingHeightMax = Double.MIN_VALUE;
        this.distanceMax = Double.MIN_VALUE;
        this.temperatureMax = Double.MIN_VALUE;
        this.pressureMax = Double.MIN_VALUE;
        this.windSpeedMax = Double.MIN_VALUE;
        this.airTempMax = Double.MIN_VALUE;
    }

    // Setter methods for minimum values
    public void setElevationMin(double elevationMin) {
        this.elevationMin = elevationMin;
    }

    public void setWindDirectionMin(double windDirectionMin) {
        this.windDirectionMin = windDirectionMin;
    }

    public void setCeilingHeightMin(double ceilingHeightMin) {
        this.ceilingHeightMin = ceilingHeightMin;
    }

    public void setDistanceMin(double distanceMin) {
        this.distanceMin = distanceMin;
    }

    public void setTemperatureMin(double temperatureMin) {
        this.temperatureMin = temperatureMin;
    }

    public void setPressureMin(double pressureMin) {
        this.pressureMin = pressureMin;
    }

    public void setWindSpeedMin(double windSpeedMin) {
        this.windSpeedMin = windSpeedMin;
    }

    public void setAirTempMin(double airTempMin) {
        this.airTempMin = airTempMin;
    }

    // Setter methods for maximum values
    public void setElevationMax(double elevationMax) {
        this.elevationMax = elevationMax;
    }

    public void setWindDirectionMax(double windDirectionMax) {
        this.windDirectionMax = windDirectionMax;
    }

    public void setCeilingHeightMax(double ceilingHeightMax) {
        this.ceilingHeightMax = ceilingHeightMax;
    }

    public void setDistanceMax(double distanceMax) {
        this.distanceMax = distanceMax;
    }

    public void setTemperatureMax(double temperatureMax) {
        this.temperatureMax = temperatureMax;
    }

    public void setPressureMax(double pressureMax) {
        this.pressureMax = pressureMax;
    }

    public void setWindSpeedMax(double windSpeedMax) {
        this.windSpeedMax = windSpeedMax;
    }

    public void setAirTempMax(double airTempMax) {
        this.airTempMax = airTempMax;
    }
    public void setElevation(double elevation) {
        this.elevation = elevation;
    }

    public void setWindDirection(double windDirection) {
        this.windDirection = windDirection;
    }

    public void setCeilingHeight(double ceilingHeight) {
        this.ceilingHeight = ceilingHeight;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public void setPressure(double pressure) {
        this.pressure = pressure;
    }

    public void setWindSpeed(double windSpeed) {
        this.windSpeed = windSpeed;
    }

    public void setAirTemp(double airTemp) {
        this.airTemp = airTemp;
    }
 // Getter methods

    public double getElevation() {
        return elevation;
    }

    public double getWindDirection() {
        return windDirection;
    }

    public double getCeilingHeight() {
        return ceilingHeight;
    }

    public double getDistance() {
        return distance;
    }

    public double getTemperature() {
        return temperature;
    }

    public double getPressure() {
        return pressure;
    }

    public double getWindSpeed() {
        return windSpeed;
    }

    public double getAirTemp() {
        return airTemp;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(elevation);
        out.writeDouble(windDirection);
        out.writeDouble(ceilingHeight);
        out.writeDouble(distance);
        out.writeDouble(temperature);
        out.writeDouble(pressure);
        out.writeDouble(windSpeed);
        out.writeDouble(airTemp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        elevation = in.readDouble();
        windDirection = in.readDouble();
        ceilingHeight = in.readDouble();
        distance = in.readDouble();
        temperature = in.readDouble();
        pressure = in.readDouble();
        windSpeed = in.readDouble();
        airTemp = in.readDouble();
    }

    @Override
    public String toString() {
        return " : "+"Elevation: " + (Double.isNaN(elevation) ? "NaN" : String.format("%.2f", elevation)) +
                "\t(Min: " + (Double.isNaN(elevationMin) ? "NaN" : String.format("%.2f", elevationMin)) +
                ", Max: " + (Double.isNaN(elevationMax) ? "NaN" : String.format("%.2f", elevationMax)) + ")\t" +
                "WindDirection: " + (Double.isNaN(windDirection) ? "NaN" : String.format("%.2f", windDirection)) +
                "\t(Min: " + (Double.isNaN(windDirectionMin) ? "NaN" : String.format("%.2f", windDirectionMin)) +
                ", Max: " + (Double.isNaN(windDirectionMax) ? "NaN" : String.format("%.2f", windDirectionMax)) + ")\t" +
                "CeilingHeight: " + (Double.isNaN(ceilingHeight) ? "NaN" : String.format("%.2f", ceilingHeight)) +
                "\t(Min: " + (Double.isNaN(ceilingHeightMin) ? "NaN" : String.format("%.2f", ceilingHeightMin)) +
                ", Max: " + (Double.isNaN(ceilingHeightMax) ? "NaN" : String.format("%.2f", ceilingHeightMax)) + ")\t" +
                "Distance: " + (Double.isNaN(distance) ? "NaN" : String.format("%.2f", distance)) +
                "\t(Min: " + (Double.isNaN(distanceMin) ? "NaN" : String.format("%.2f", distanceMin)) +
                ", Max: " + (Double.isNaN(distanceMax) ? "NaN" : String.format("%.2f", distanceMax)) + ")\t" +
                "Temperature: " + (Double.isNaN(temperature) ? "NaN" : String.format("%.2f", temperature)) +
                "\t(Min: " + (Double.isNaN(temperatureMin) ? "NaN" : String.format("%.2f", temperatureMin)) +
                ", Max: " + (Double.isNaN(temperatureMax) ? "NaN" : String.format("%.2f", temperatureMax)) + ")\t" +
                "Pressure: " + (Double.isNaN(pressure) ? "NaN" : String.format("%.2f", pressure)) +
                "\t(Min: " + (Double.isNaN(pressureMin) ? "NaN" : String.format("%.2f", pressureMin)) +
                ", Max: " + (Double.isNaN(pressureMax) ? "NaN" : String.format("%.2f", pressureMax)) + ")\t" +
                "WindSpeed: " + (Double.isNaN(windSpeed) ? "NaN" : String.format("%.2f", windSpeed)) +
                "\t(Min: " + (Double.isNaN(windSpeedMin) ? "NaN" : String.format("%.2f", windSpeedMin)) +
                ", Max: " + (Double.isNaN(windSpeedMax) ? "NaN" : String.format("%.2f", windSpeedMax)) + ")\t" +
                "AirTemp: " + (Double.isNaN(airTemp) ? "NaN" : String.format("%.2f", airTemp)) +
                "\t(Min: " + (Double.isNaN(airTempMin) ? "NaN" : String.format("%.2f", airTempMin)) +
                ", Max: " + (Double.isNaN(airTempMax) ? "NaN" : String.format("%.2f", airTempMax)) + ")" +"\n\n";
    }



    
}
