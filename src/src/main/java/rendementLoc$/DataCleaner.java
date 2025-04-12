package rendementLoc$;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;


public class DataCleaner {

    // Méthode pour nettoyer le Dataset en ne conservant que les lignes correspondant à une région donnée
    public static Dataset<Row> filterRegion(Dataset<Row> data, String regionCode) {
        // Identifier la colonne région (REG) et ne conserver que les lignes correspondant à la région spécifiée
        return data.filter(data.col("REG").equalTo(regionCode));
    }
    
    // Méthode pour nettoyer dynamiquement et obtenir les données filtrées
    public static Dataset<Row> cleanData(Dataset<Row> data, String regionCode) {
        // Appliquer le filtre pour ne conserver que les lignes de la région spécifiée
        return filterRegion(data, regionCode);
    }

    // Méthode pour filtrer les données et sauvegarder dans un fichier CSV unique
    public static void saveFilteredData(Dataset<Row> data, String regionCode, String outputPath) {
        // Filtrer les données pour la région spécifiée
        Dataset<Row> filteredData = cleanData(data, regionCode);

        // Sauvegarder les données filtrées dans un fichier CSV (un seul fichier)
        filteredData.coalesce(1)  // Regrouper toutes les partitions en un seul fichier
                .write()
                .option("header", "true")  // Ajouter l'en-tête au fichier de sortie
                .option("delimiter", ";")  // Utiliser le même séparateur que le fichier d'entrée
                .mode(SaveMode.Overwrite)  // Permet de remplacer le fichier s'il existe déjà
                .csv(outputPath);  // Sauvegarder au format CSV
    }
    
    /**
     * Charge le CSV DVF et le CSV des correspondances (contenant Code Postal, Commune, Code INSEE, Code Département, Région, etc.),
     * filtre pour ne garder que les lignes dont le code postal et la commune figurent dans les correspondances des régions désirées
     * (par exemple, LANGUEDOC‑ROUSSILLON ou MIDI‑PYRENEES)
     * et dont la colonne "Nature mutation" vaut "Vente".
     * Ensuite, filtre pour conserver uniquement les enregistrements où "Surface reelle bati" est renseignée,
     * joint pour récupérer les colonnes "Code INSEE" et "Code Département" (renommé en "DEP"),
     * puis agrège les ventes pour une même adresse (groupBy sur "No voie", "Voie", "CP_VF", "Date mutation", "Commune", "Code INSEE", "DEP")
     * en additionnant "Valeur fonciere" et "Surface reelle bati". Enfin, élimine les groupes où la surface agrégée est nulle ou égale à 0.
     *
     * @param spark          La session Spark.
     * @param dvfFilePath    Chemin vers le CSV DVF.
     * @param regionFilePath Chemin vers le CSV des correspondances.
     * @return Un Dataset agrégé avec la somme des valeurs foncières et la somme des surfaces, pour les adresses correspondant aux régions désirées.
     */
    public static Dataset<Row> cleanAndAggregateValuesFonciere(SparkSession spark, String dvfFilePath, String regionFilePath) {
        
        // 1) Charger le CSV DVF
        Dataset<Row> dvfDF = spark.read()
            .option("header", "true")
            .option("delimiter", ";")
            .csv(dvfFilePath);
        
        // Filtrer pour ne conserver que les lignes dont "Nature mutation" vaut "Vente"
        dvfDF = dvfDF.filter(functions.col("Nature mutation").equalTo("Vente"));
        
        // 2) Charger le CSV des correspondances
        Dataset<Row> regionDF = spark.read()
            .option("header", "true")
            .option("delimiter", ";")
            .csv(regionFilePath);
        
        // 3) Filtrer le CSV des correspondances pour ne garder que celles des régions désirées.
        // Ici, on suppose que la colonne "Région" contient exactement des chaînes telles que "['LANGUEDOC-ROUSSILLON']".
        Dataset<Row> regionFiltered = regionDF.filter(
            functions.col("Région").isin("['LANGUEDOC-ROUSSILLON']", "['MIDI-PYRENEES']")
        );
        
        // 4) Harmoniser les colonnes pour le join :
        // - Dans DVF, renommer "Code postal" en "CP_VF".
        // - Dans regionFiltered, renommer "Code Postal" en "CP_region" et "Commune" en "Commune_R".
        Dataset<Row> dvfRenamed = dvfDF.withColumnRenamed("Code postal", "CP_VF");
        regionFiltered = regionFiltered.withColumnRenamed("Code Postal", "CP_region")
                                       .withColumnRenamed("Commune", "Commune_R")
                                       .withColumnRenamed("Code Département", "DEP");
        
        // 5) Faire un join inner sur le code postal et la commune
        // Condition : CP_VF == CP_region AND DVF.Commune == Commune_R
        Dataset<Row> joinedDF = dvfRenamed.join(
            regionFiltered,
            dvfRenamed.col("CP_VF").equalTo(regionFiltered.col("CP_region"))
                .and(dvfRenamed.col("Commune").equalTo(regionFiltered.col("Commune_R"))),
            "inner"
        );
        
        // 6) Filtrer pour ne conserver que les lignes où "Surface reelle bati" est renseignée (non null et non vide)
        Dataset<Row> filteredJoinedDF = joinedDF.filter(
            functions.col("Surface reelle bati").isNotNull()
                .and(functions.col("Surface reelle bati").notEqual(""))
        );
        
        // 7) Agréger par adresse et par date de mutation en incluant la commune, le Code INSEE et le DEP 
        // on regroupe également les lignes qui correspondent à la meme vente correspondante
        // mais ca rend difficile le regroupement par appartement et maison à cause des lignes de dépendances
        Dataset<Row> aggregatedDF = filteredJoinedDF
            // Conversion de "Valeur fonciere" (remplacer la virgule par un point et caster en double)
            .withColumn("valeur_fonciere_num", 
                functions.expr("cast(regexp_replace(`Valeur fonciere`, ',', '.') as double)"))
            // Conversion de "Surface reelle bati"
            .withColumn("surface_reelle_bati_num", 
                functions.expr("cast(regexp_replace(`Surface reelle bati`, ',', '.') as double)"))
            .groupBy("No voie", "Voie", "CP_VF", "Commune","Date mutation", "Code INSEE", "DEP")
            .agg(
                functions.sum("valeur_fonciere_num").alias("valeur_fonciere_somme"),
                functions.sum("surface_reelle_bati_num").alias("surface_reelle_bati_somme")
            );
        
        // 8) Filtrer pour ne conserver que les lignes où la somme de la surface réelle bâtie est strictement > 0.
        aggregatedDF = aggregatedDF.filter(functions.col("surface_reelle_bati_somme").gt(0));
        
        return aggregatedDF;
    }
    
    
}
