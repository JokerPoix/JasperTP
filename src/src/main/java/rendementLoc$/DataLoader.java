package rendementLoc$;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataLoader {
    public static Dataset<Row> loadData(SparkSession spark, String filePath) {
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .option("sep", "\t")  // Délimiteur tabulation
                .option("encoding", "UTF-8")
                .option("mode", "DROPMALFORMED")  // Ignorer les lignes corrompues
                .csv(filePath);

        // Forcer la colonne `code` en String
        data = data.withColumn("code", data.col("code").cast("string"));

        // Vérifier les colonnes détectées
        System.out.println("💡 Colonnes détectées : " + String.join(", ", data.columns()));

        // Afficher un aperçu des 5 premières lignes
        data.show(5, false);

        return data;
    }
}
