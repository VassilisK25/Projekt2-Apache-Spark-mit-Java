import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.Array;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

public class Spark_SQL {
    public static void main(String[] args) throws IOException {

        // Die URL zu der Excel-Datei aus dem Internet
        String url = "https://www.berlin.de/sen/arbeit/weiterbildung/bildungszeit/suche/index.php/index/all.xls?q=";
        String url2 = "https://mobidata-bw.de/daten/eco-counter/eco_counter_fahrradzaehler.json";


        // Query-Abfragen auf die Excel-URL
        String query1 = "SELECT * FROM excel_table";
        //String query2 = "SELECT DISTINCT Veranstaltung FROM excel_table";
        //String query3 = "SELECT * FROM excel_table WHERE NOT Veranstaltungslink = 'NULL'";

        // Methode zur Verarbeitung der Tabelle wird mit drei unterschiedlichen
        // Queries aufgerufen
        Spark_SQL a = new Spark_SQL();
        //a.verarbeitungXLS(url, query1);
        //a.verarbeitungXLS(localPath, query2);
        //a.verarbeitungXLS(localPath, query3);

        String query4 = "SELECT * FROM json_table WHERE standort = 'Stadt Tübingen' AND `zählstand` < 150";
        a.verarbeitungJSON(url2, query4);
    }

    public void verarbeitungXLS(String path, String query){
        String localPath = "dateiPfad.xlsx";
        // es wird eine locale Kopie erstellt
        try (InputStream in = new URL(path).openStream()) {
            Files.copy(in, Paths.get(localPath), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Fehler beim Herunterladen der Datei: " + e.getMessage());
            return;
        }
        SparkSession spark = SparkSession.builder()
                .appName("Excel SQL Query")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1") // Host und Port
                .getOrCreate();

        // Erstellung eines DataSet
        Dataset<Row> df = spark.read()
                .format("com.crealytics.spark.excel")   // DataSource-Name der Bibliothek
                .option("header", "true")               // erste Zeile als Header
                .option("inferSchema", "true")          // Typen automatisch erkennen
                .option(localPath, "'Tabelle1'!A1")   // Bereich (Sheet1, ab Zelle A1)
                .load(localPath);

        df.printSchema();

        // Anzahl der Zeilen die angezeigt werden sollen
        df.show(15);

        df.createOrReplaceTempView("excel_table");

        // Abfrage einer Query auf die Datenstruktur
        Dataset<Row> result1 = spark.sql(query);
        result1.show();
        spark.stop();
    }

    public void verarbeitungJSON(String path, String query){
        String localPath = "dateiPfad.json";
        // es wird eine locale Kopie erstellt
        try (InputStream in = new URL(path).openStream()) {
            Files.copy(in, Paths.get(localPath), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Fehler beim Herunterladen der Datei: " + e.getMessage());
            return;
        }
        SparkSession spark = SparkSession.builder()
                .appName("Excel SQL Query")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1") // Host und Port
                .getOrCreate();

        // Schema der JSON-Datei
        StructType schema = new StructType(new StructField[]{
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("iso_timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("zählstand", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("stand", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("standort", DataTypes.StringType, true, Metadata.empty()),
                new StructField("channel_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("channel_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("counter_site", DataTypes.StringType, true, Metadata.empty()),
                new StructField("counter_site_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("domain_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("domain_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("longitude", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("latitude", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timezone", DataTypes.StringType, true, Metadata.empty()),
                new StructField("interval", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("counter_serial", DataTypes.StringType, true, Metadata.empty())
        });
        // Erstellung eines DataSet
        Dataset<Row> df = spark.read()
                .option("multiline", "true") // für JSON-Format multiline
                .schema(schema)
                .json(localPath);

        df.createOrReplaceTempView("json_table");
        df.show(50);
        df.printSchema();

        // Nur bestimmte Spalten anzeigen
        Dataset<Row> selected = spark.sql(query);
        selected.show();
        spark.stop();
    }
}
