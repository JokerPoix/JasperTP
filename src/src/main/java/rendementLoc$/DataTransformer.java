package rendementLoc$;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SaveMode;
import java.io.File;
import java.util.Objects;

public class DataTransformer {

    // Calcul de la moyenne par département et par année sur des données déjà filtrées
    public static void saveAverageByDepartmentAndYear(Dataset<Row> filteredData, String outputPath) {
        // Transformer les colonnes pour convertir les valeurs numériques et les renommer
        Dataset<Row> convertedData = filteredData
            .withColumn("loypredm2_conv", functions.expr("cast(regexp_replace(`loypredm2`, ',', '.') as double)"))
            .withColumn("lwr_IPm2_conv", functions.expr("cast(regexp_replace(`lwr.IPm2`, ',', '.') as double)"))
            .withColumn("upr_IPm2_conv", functions.expr("cast(regexp_replace(`upr.IPm2`, ',', '.') as double)"));

        // Agréger par département ("DEP") et par année ("année")
        Dataset<Row> avgByDeptAndYear = convertedData.groupBy("DEP", "année")
            .agg(
                functions.avg(functions.col("loypredm2_conv")).alias("avg_loypredm2"),
                functions.avg(functions.col("lwr_IPm2_conv")).alias("avg_lwr_ipm2"),
                functions.avg(functions.col("upr_IPm2_conv")).alias("avg_upr_ipm2")
            );

        saveAsSingleCSV(avgByDeptAndYear, outputPath, "averageEstimationLocationByDept.csv");
    }

    // Calcul de la moyenne par ville et par année sur des données déjà filtrées
    public static void saveAverageByCityAndYear(Dataset<Row> filteredData, String outputPath) {
        // Transformer les colonnes pour convertir les valeurs numériques et les renommer
        Dataset<Row> convertedData = filteredData
            .withColumn("loypredm2_conv", functions.expr("cast(regexp_replace(`loypredm2`, ',', '.') as double)"))
            .withColumn("lwr_IPm2_conv", functions.expr("cast(regexp_replace(`lwr.IPm2`, ',', '.') as double)"))
            .withColumn("upr_IPm2_conv", functions.expr("cast(regexp_replace(`upr.IPm2`, ',', '.') as double)"));

        // Agréger par ville ("LIBGEO") et par année ("année")
        Dataset<Row> avgByCityAndYear = convertedData.groupBy("LIBGEO", "INSEE_C", "année")
        	.agg(
                functions.avg(functions.col("loypredm2_conv")).alias("avg_loypredm2"),
                functions.avg(functions.col("lwr_IPm2_conv")).alias("avg_lwr_ipm2"),
                functions.avg(functions.col("upr_IPm2_conv")).alias("avg_upr_ipm2")
            );

        // Sauvegarder le résultat dans un CSV unique
        saveAsSingleCSV(avgByCityAndYear, outputPath, "averageEstimationLocationByCity.csv");

    }
    
    /**
     * Ajoute une colonne "année" (extraite de "Date mutation") au Dataset agrégé.
     * On suppose ici que "Date mutation" est au format "dd/MM/yyyy".
     * Puis, sauvegarde le Dataset dans un fichier CSV unique.
     * 
     * @param aggregatedDF Le Dataset résultant de DataCleaner.cleanAndAggregateDVF.
     * @param outputFolder Le dossier de sortie.
     * @param fileName Le nom souhaité du fichier CSV final.
     */
    public static void addYearColumnAndSave(Dataset<Row> aggregatedDF, String outputFolder, String fileName) {
        // Ajouter la colonne "année" en convertissant "Date mutation" au format date et en en extrayant l'année.
        Dataset<Row> withYearDF = aggregatedDF.withColumn("année",
                functions.year(functions.to_date(functions.col("Date mutation"), "dd/MM/yyyy")));
        
        // Sauvegarder dans un CSV unique via la méthode utilitaire.
        saveAsSingleCSV(withYearDF, outputFolder, fileName);
    }
    
    public static void saveAsSingleCSV(Dataset<Row> dataset, String outputFolder, String fileName) {
        // 1) Chemin du dossier temporaire où Spark écrit
        String tempPath = outputFolder + "/temp_output";

        // 2) Écriture dans le dossier temporaire
        dataset.coalesce(1)
               .write()
               .option("header", "true")
               .option("delimiter", ";")
               .mode("overwrite")
               .csv(tempPath);

        // 3) Dans ce dossier, Spark aura créé un fichier du style part-00000-...
        //    On va le repérer et le renommer
        File tempDir = new File(tempPath);
        File finalDir = new File(outputFolder);

        // Récupérer le fichier CSV créé (part-xxxxx)
        File[] files = tempDir.listFiles((dir, name) -> name.startsWith("part-"));
        if (files != null && files.length > 0) {
            // Le renommage
            File partFile = files[0];
            File outputFile = new File(finalDir, fileName); // par ex. "averageByCity.csv"
            partFile.renameTo(outputFile);
        }

        // 4) Supprimer le dossier temporaire et les fichiers restants
        for (File f : Objects.requireNonNull(tempDir.listFiles())) {
            f.delete();
        }
        tempDir.delete();
    }
}
