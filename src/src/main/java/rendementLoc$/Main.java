package rendementLoc$;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.spark.sql.functions;

import rendementLoc$.DataLoader;


public class Main {
	public static void main(String[] args) {
	
	// Initialisation de Spark
    SparkSession spark = SparkSession.builder()
            .appName("rendementLoc$")
            .config("spark.master", "local[*]")  // Utilisation de tous les cœurs CPU disponibles
            .config("spark.driver.memory", "16g")  // Augmenter la mémoire allouée au driver
            .config("spark.executor.memory", "8g")  // Ajouter de la mémoire aux exécutors
            .config("spark.executor.cores", "4")  // Augmenter les cœurs d'exécution
            .config("spark.sql.shuffle.partitions", "16")  // Augmenter les partitions pour éviter la saturation de mémoire
            .getOrCreate();
    
    // Définition des chemins de fichiers
    String inputEstimationLoyerPath = "src/main/resources/inputs/EstimationLoyer";
    String inputValeursFoncieresPath = "src/main/resources/inputs/ValeursFoncieres";
    String outputFolderPath = "src/main/resources/outputs";
    String parquetEstimationLoyerFilePath = outputFolderPath + "/esmationLoyer.parquet";;
    String parquetValeursFoncieresFilePath = outputFolderPath + "/valeursFoncieres.parquet";;

    
    Dataset<Row> data;
    
    // 🔄 Fusion des fichiers EstimationLoyer (si non déjà généré)
    if (!Files.exists(Paths.get(outputFolderPath + "/estimationLoyer.csv"))) {
        Dataset<Row> estimationLoyerDF = DataAggregator.mergeCSVFromFolder(spark, inputEstimationLoyerPath);
        DataAggregator.saveAsCSV(estimationLoyerDF, outputFolderPath, "estimationLoyer.csv");
    } else {
        System.out.println("✅ estimationLoyer.csv existe déjà, pas de traitement.");
    }

    // 🔄 Conversion et fusion des fichiers txt (Valeurs Foncieres)
    if (!Files.exists(Paths.get(outputFolderPath + "/valeursFoncieres.csv"))) {
        Dataset<Row> valeursFoncieresDF = DataAggregator.convertAndMergeTXTtoCSV(spark, inputValeursFoncieresPath);
        DataAggregator.saveAsCSV(valeursFoncieresDF, outputFolderPath, "valeursFoncieres.csv");
    } else {
        System.out.println("✅ valeursFoncieres.csv existe déjà, pas de traitement.");
    }



    
}
}
