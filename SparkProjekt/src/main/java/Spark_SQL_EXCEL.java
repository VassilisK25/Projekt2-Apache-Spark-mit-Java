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

public class Spark_SQL_EXCEL {
    public static void main(String[] args) throws IOException {

        // Die URL zu der Excel-Datei aus dem Internet
        String url = "https://www.berlin.de/sen/arbeit/weiterbildung/bildungszeit/suche/index.php/index/all.xls?q=";


        // Query-Abfragen auf die Excel-URL
        String query1 = "SELECT * FROM excel_table";
        String query2 = "SELECT DISTINCT Veranstaltung FROM excel_table";
        String query3 = "SELECT * FROM excel_table WHERE NOT Veranstaltungslink = 'NULL'";

        // Methode zur Verarbeitung der Tabelle wird mit drei unterschiedlichen
        // Queries aufgerufen
        Spark_SQL_EXCEL a = new Spark_SQL_EXCEL();
        a.verarbeitungXLS(url, query1);
        a.verarbeitungXLS(url, query2);
        a.verarbeitungXLS(url, query3);

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
}
