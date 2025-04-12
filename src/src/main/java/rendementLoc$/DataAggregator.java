package rendementLoc$;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import java.util.Objects;
import java.io.File;

public class DataAggregator {

	/**
     * Fusionne tous les fichiers CSV présents dans un dossier en un seul Dataset Spark.
     * @param spark La session Spark utilisée pour le traitement.
     * @param folderPath Le chemin du dossier contenant les fichiers CSV.
     * @return Un Dataset<Row> fusionné avec une colonne "année" ajoutée.
     */
    public static Dataset<Row> mergeCSVFromFolder(SparkSession spark, String folderPath) {
        File folder = new File(folderPath);
        File[] csvFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            throw new RuntimeException("Aucun fichier CSV trouvé dans le dossier : " + folderPath);
        }

        Dataset<Row> merged = null;
        for (File file : csvFiles) {
            // Extraire l'année du nom du fichier
            String fileName = file.getName();
            String year = fileName.replaceAll("\\D+", ""); // Récupérer les chiffres du nom du fichier

            // Charger le fichier CSV
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("sep", ";")
                    .option("inferSchema", "true")
                    .csv(file.getPath());

            // Ajouter la colonne "année" au Dataset
            df = df.withColumn("année", functions.lit(year));

            // Fusionner les fichiers
            merged = (merged == null) ? df : merged.unionByName(df);
        }

        return merged;
    }


    /**
     * Lit tous les fichiers .txt au format DVF (séparés par |), les convertit en CSV et les fusionne.
     * @param spark La session Spark utilisée pour le traitement.
     * @param folderPath Le chemin du dossier contenant les fichiers .txt.
     * @return Un Dataset<Row> fusionné contenant les données des fichiers .txt.
     */
    public static Dataset<Row> convertAndMergeTXTtoCSV(SparkSession spark, String folderPath) {
        File folder = new File(folderPath);
        File[] txtFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".txt"));

        if (txtFiles == null || txtFiles.length == 0) {
            throw new RuntimeException("Aucun fichier TXT trouvé dans le dossier : " + folderPath);
        }

        Dataset<Row> merged = null;
        for (File file : txtFiles) {
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("sep", "|")
                    .option("inferSchema", "true")
                    .csv(file.getPath());

            merged = (merged == null) ? df : merged.unionByName(df);
        }

        return merged;
    }

    /**
     * Sauvegarde un Dataset<Row> dans un fichier CSV unique nommé selon le contexte.
     * @param dataset Le Dataset à sauvegarder.
     * @param outputPath Le chemin de dossier de destination (le fichier CSV sera nommé automatiquement).
     * @param filename Le nom souhaité pour le fichier final (ex: "valeursFoncieres.csv").
     */
    public static void saveAsCSV(Dataset<Row> dataset, String outputPath, String filename) {
        String tempPath = outputPath + "/temp_output";

        // Écriture dans un répertoire temporaire avec Spark
        dataset.coalesce(1)
               .write()
               .option("header", "true")
               .option("sep", ";")
               .mode("overwrite")
               .csv(tempPath);

        // Renommer le fichier généré automatiquement par Spark en nom voulu
        File tempDir = new File(tempPath);
        File finalDir = new File(outputPath);
        File[] files = tempDir.listFiles((dir, name) -> name.endsWith(".csv"));
        if (files != null && files.length > 0) {
            files[0].renameTo(new File(finalDir, filename));
        }

        // Supprimer le dossier temporaire
        for (File f : Objects.requireNonNull(tempDir.listFiles())) {
            f.delete();
        }
        tempDir.delete();
    }
}