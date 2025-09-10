
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import java.util.ArrayList;
import java.util.List;

public class Histogram {
    public static void main(String[] args) {
        System.out.println("Datensatz zum Histogram");
        // Die URL zu der Excel-Datei aus dem Internet
        String url = "https://mobidata-bw.de/daten/portal/RV_Zaehl/Verkehrszaehlungen_RV.xlsx";

        // Methode zur Verarbeitung der Tabelle und Erstellung des Histograms
        Histogram a = new Histogram();
        a.erstelleHistogram(url);
    }

    public void erstelleHistogram(String path){

        SparkSession spark = SparkSession.builder()
                .appName("Histogram")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1") // Host und Port
                .getOrCreate();

        // Erstellung eines DataSet
        Dataset<Row> df = spark.read()
                .format("com.crealytics.spark.excel")   // DataSource-Name der Bibliothek
                .option("header", "true")               // erste Zeile als Header
                .option("inferSchema", "true")          // Typen automatisch erkennen
                .option(path, "'Stadt Ravensburg'!A1")   // Bereich (Sheet1, ab Zelle A1)
                .load(path);

        df.printSchema();

        // Anzahl der Zeilen die angezeigt werden sollen
        df.show(15);

        df.createOrReplaceTempView("excel_table");


        //----------------------------------------------------------------------------------------------//
        // Datenaufbereitung

        // Umwandlung der Inhalte in Spalte Datum_1 in das Format YYYY-MM
        // umbenennen der geänderten Spalte in "Datum_1_neu"
        Dataset<Row> dfMitMonat = df.withColumn("Datum_1_neu",
                functions.date_format(df.col("Datum_1"), "yyyy-MM"));

        // Häufigkeit der Daten ermitteln nach JahrMonat
        Dataset<Row> histogramData = dfMitMonat.groupBy("Datum_1_neu").count().orderBy("Datum_1_neu");

        List<String> xData = new ArrayList<>();
        List<Number> yData = new ArrayList<>();

        // Ermitteln der Häufigkeit des Datums nach YYYY-MM
        for (Row row : histogramData.collectAsList()) {
            String monat = row.getAs("Datum_1_neu");
            Long count = row.getAs("count");

            // leere Spalten nicht berücksichtigen
            if (monat != null && count != null) {
                xData.add(monat);
                yData.add(count);
            }
        }

        //--------------------------------------------------------------------------------------//
        // Erstellung des Graphen
        CategoryChart chart = new CategoryChartBuilder()
                .width(800).height(600)
                .title("Histogramm der Anzahl der Datenerhebungen nach Monat")
                .xAxisTitle("Stichtag der Datenerhebung")
                .yAxisTitle("Absolute Häufigkeit")
                .build();

        chart.addSeries("Häufigkeit nach Datum", xData, yData);

        // Interface das Graphen erstellt
        new SwingWrapper<>(chart).displayChart();

        spark.stop();
    }
}
