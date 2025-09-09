
// dieses Programm soll ein Histogram aus einer bestimmten Tabellenspalte einer Exceldatei
// erstellen und abbilden
// https://www.daten-bw.de/web/guest/suchen/-/details/strassenverkehrszahlungen-stadt-ravensburg
// Programm morgen früh zuerst ausführen und Fehlersuche

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class Histogram {
    public static void main(String[] args) {
        System.out.println("Datensatz zum Histogram");
        // Die URL zu der Excel-Datei aus dem Internet
        String url = "https://mobidata-bw.de/daten/portal/RV_Zaehl/Verkehrszaehlungen_RV.xlsx";

        // Methode zur Verarbeitung der Tabelle wird mit drei unterschiedlichen
        // Queries aufgerufen
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

        spark.stop();
    }
}
