package rendementLoc$;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class UpdateCodeRegion {
    public static void update(SparkSession spark, String inputFilePath, String outputFilePath) {
        // Lecture du fichier d'origine
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("delimiter", "\t")
                .csv(inputFilePath);

        // Remplacement de 73 → 76 dans "Code Région"
        Dataset<Row> updatedDF = df.withColumn("Code Région",
                functions.when(functions.col("Code Région").equalTo("73"), "76")
                        .otherwise(functions.col("Code Région"))
        );

        // Sauvegarde dans un nouveau fichier CSV
        updatedDF.coalesce(1) 
                .write()
                .option("header", "true")
                .option("delimiter", "\t") 
                .mode("overwrite")
                .csv(outputFilePath);

        System.out.println("✅ Fichier transformé sauvegardé dans : " + outputFilePath);
    }
}
