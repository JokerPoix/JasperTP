package rendementLoc$;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.nio.file.Files;
import java.nio.file.Paths;

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
    String inputFolderPath = "src/main/resources/inputs";
    String inputEstimationLoyerPath = "src/main/resources/inputs/EstimationLoyer";
    String inputValeursFoncieresPath = "src/main/resources/inputs/ValeursFoncieres";
    String outputFolderPath = "src/main/resources/outputs";
    String csvCodePostalInseeReferences = inputFolderPath + "/correspondanceCodeInseeCodePostal.csv";;

        
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

    // Charger le fichier estimationLoyer.csv
    Dataset<Row> data = spark.read().option("header", "true").option("delimiter", ";").csv(outputFolderPath + "/estimationLoyer.csv");

    // Filtrer les lignes correspondant à la région (par exemple, Occitanie)
    String regionCode = "76";  // Mettez ici le code de la région désirée
    Dataset<Row> filteredData = DataCleaner.filterRegion(data, regionCode);

    // Sauvegarder les données filtrées dans un CSV unique (optionnel)
    DataCleaner.saveFilteredData(data, regionCode, outputFolderPath + "/estimationLoyerOccitanie.csv");

    // Calculer et sauvegarder les moyennes par département (à partir du jeu déjà filtré)
    DataTransformer.saveAverageByDepartmentAndYear(filteredData, outputFolderPath);

    // Calculer et sauvegarder les moyennes par ville (à partir du jeu déjà filtré)
    DataTransformer.saveAverageByCityAndYear(filteredData, outputFolderPath);

    Dataset<Row> aggregatedDF = DataCleaner.cleanAndAggregateValuesFonciere(spark, outputFolderPath + "/valeursFoncieres.csv", csvCodePostalInseeReferences);
    DataTransformer.addYearColumnAndSave(aggregatedDF, outputFolderPath, "valeursFoncieresCleanedWithYearSurfacesINSEEAndSums.csv");
    System.out.println("✅ Les traitements sur les estimations loyers(filtrage et agrégations) ont été réalisés pour la région " + regionCode + ".");
  
    //Calcul du rendement locatif par département --

    // 1) Charger le CSV "valeursFoncieresCleanedWithYearSurfacesINSEEAndSums.csv"
    //    qui contient DEP, année, surface_reelle_bati_somme, valeur_fonciere_somme, etc.
    String cleanedVFFile = outputFolderPath + "/valeursFoncieresCleanedWithYearSurfacesINSEEAndSums.csv";
    Dataset<Row> aggregatedDFWithYear = spark.read()
            .option("header", "true")
            .option("delimiter", ";")
            .csv(cleanedVFFile);

    // 2) Charger le CSV "averageEstimationLocationByDept.csv"
    //    qui contient DEP, année, avg_loypredm2
    String avgLoyerDeptFile = outputFolderPath + "/averageEstimationLocationByDept.csv";
    Dataset<Row> avgDeptDF = spark.read()
            .option("header", "true")
            .option("delimiter", ";")
            .csv(avgLoyerDeptFile);

    // 3) Calculer le rendement locatif par département et par année
    Dataset<Row> yieldByDept = RentalYieldGenerator.computeRentalYieldByDepartment(aggregatedDFWithYear, avgDeptDF);

    // 4) Sauvegarder le résultat dans un unique fichier CSV : "rentalYieldByDept.csv"

    // Sauvegarde du rendement locatif dans un fichier unique en réutilisant la méthode existante
    DataTransformer.saveAsSingleCSV(yieldByDept, outputFolderPath, "rentalYieldByDept.csv");


    System.out.println("✅ Calcul du rendement locatif par département terminé.");
    
    String avgLoyerCitytFile = outputFolderPath + "/averageEstimationLocationByCity.csv";
    Dataset<Row> avgCitytDF = spark.read()
            .option("header", "true")
            .option("delimiter", ";")
            .csv(avgLoyerCitytFile);
    
    Dataset<Row> yieldByCity = RentalYieldGenerator.computeRentalYieldByCity(aggregatedDFWithYear, avgCitytDF);

    DataTransformer.saveAsSingleCSV(yieldByCity, outputFolderPath, "rentalYieldByCity.csv");

    // ---- Partie pour insérer les CSV dans la base de données via JDBC ----
    // Dans une BDD my sql connecté en root on peut faire ca:
    // DROP USER IF EXISTS 'poix'@'localhost';
	// CREATE USER 'poix'@'localhost' IDENTIFIED BY 'poix';
	// GRANT ALL PRIVILEGES ON rendement.* TO 'poix'@'localhost';
	// FLUSH PRIVILEGES;

    
    String jdbcUrl = "jdbc:mysql://localhost:3306/rendement?useSSL=false&serverTimezone=UTC";
    java.util.Properties connectionProperties = new java.util.Properties();
    connectionProperties.put("user", "poix");
    connectionProperties.put("password", "poix");
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver");

    Dataset<Row> rentalYieldByDeptDF = spark.read()
            .option("header", "true")
            .option("delimiter", ";")
            .csv(outputFolderPath + "/rentalYieldByDept.csv");

    rentalYieldByDeptDF.write()
            .mode(SaveMode.Overwrite)
            .jdbc(jdbcUrl, "rental_yield_by_department", connectionProperties);

    Dataset<Row> rentalYieldByCityDF = spark.read()
            .option("header", "true")
            .option("delimiter", ";")
            .csv(outputFolderPath + "/rentalYieldByCity.csv");

    rentalYieldByCityDF.write()
            .mode(SaveMode.Overwrite)
            .jdbc(jdbcUrl, "rental_yield_by_city", connectionProperties);

    System.out.println("✅ rentalYieldByCity inséré dans la base de données.");

    spark.stop();
	}
}
	
	

