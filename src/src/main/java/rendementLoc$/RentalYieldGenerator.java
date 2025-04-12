package rendementLoc$;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * RentalYieldGenerator computes the rental yield per department and per year.
 *
 * The rental yield is calculated as follows:
 *
 *      RentalYield(%) = ((avg_loypredm2 * total_surface * 12) / total_value) * 100
 *
 * where:
 * - avg_loypredm2 is the average monthly rent per m² (from an external dataset)
 * - total_surface is the total built surface aggregated by department and year
 * - total_value   is the total acquisition price aggregated by department and year
 *
 * The join is performed on the columns DEP and année.
 */
public class RentalYieldGenerator {

    /**
     * Computes the rental yield by department and by year.
     *
     * @param aggregatedDF Dataset obtained from DataCleaner that contains at least:
     *                     DEP, Date mutation, valeur_fonciere_somme, surface_reelle_bati_somme.
     * @param avgLoyerDF   Dataset containing the average monthly rent per m², with columns:
     *                     DEP, année, avg_loypredm2.
     * @return A Dataset<Row> with columns DEP, année, avg_loypredm2, total_surface, total_value and rental_yield (in %).
     */
    public static Dataset<Row> computeRentalYieldByDepartment(Dataset<Row> aggregatedDF, Dataset<Row> avgLoyerDF) {
        // 1. Add a column "année" extracted from "Date mutation"
        Dataset<Row> dfWithYear = aggregatedDF.withColumn("année",
                functions.year(functions.to_date(functions.col("Date mutation"), "dd/MM/yyyy")));

        // 2. Aggregate by department (DEP) and year to sum the built surface and acquisition price
        Dataset<Row> deptAgg = dfWithYear.groupBy("DEP", "année")
                .agg(
                        functions.sum("surface_reelle_bati_somme").alias("total_surface"),
                        functions.sum("valeur_fonciere_somme").alias("total_value")
                );

        // 3. Join with the dataset containing avg_loypredm2 on DEP and année
        Dataset<Row> joined = deptAgg.join(
                avgLoyerDF,
                deptAgg.col("DEP").equalTo(avgLoyerDF.col("DEP"))
                        .and(deptAgg.col("année").equalTo(avgLoyerDF.col("année"))),
                "inner"
        );

        // 4. Compute the rental yield using the formula:
        //    ((avg_loypredm2 * total_surface * 12) / total_value) * 100
        //    and round the result to 2 decimal places.
        Dataset<Row> yieldDF = joined.withColumn("rental_yield",
                functions.round(functions.expr("((avg_loypredm2 * total_surface * 12) / total_value) * 100"), 2)
        );
        // 5. Select the desired columns
        return yieldDF.select(
                avgLoyerDF.col("DEP"),
                deptAgg.col("année"),
                functions.col("rental_yield")
        );
    }
    /**
     * Computes the rental yield by city (commune) and by year.
     *
     * This method assumes:
     * - The aggregated dataset contains at least: Code INSEE, Date mutation, valeur_fonciere_somme, surface_reelle_bati_somme.
     * - The external dataset for average rent contains the columns: INSEE_C, année, avg_loypredm2, LIBGEO.
     * 
     * The join is performed on the INSEE code and year, after renaming INSEE_C to "Code_INSEE".
     *
     * @param aggregatedDF Dataset from DataCleaner containing city-level aggregated data.
     * @param avgCityDF    Dataset containing the average monthly rent per m² by city, with columns:
     *                     INSEE_C, année, avg_loypredm2, LIBGEO.
     * @return A Dataset<Row> with columns LIBGEO, Code_INSEE, année and rental_yield (in %, rounded to 2 decimals).
     */
    public static Dataset<Row> computeRentalYieldByCity(Dataset<Row> aggregatedDF, Dataset<Row> avgCityDF) {
        // 1. Add a column "année" extracted from "Date mutation"
        Dataset<Row> dfWithYear = aggregatedDF.withColumn("année",
                functions.year(functions.to_date(functions.col("Date mutation"), "dd/MM/yyyy")));
        
        // 2. Aggregate by city using "Code INSEE" and year to obtain total surface and total value
        Dataset<Row> cityAgg = dfWithYear.groupBy("Code INSEE", "année")
                .agg(
                        functions.sum("surface_reelle_bati_somme").alias("total_surface"),
                        functions.sum("valeur_fonciere_somme").alias("total_value")
                );
        
        // 3. Rename the column INSEE_C in the average rent dataset to "Code_INSEE" for consistency
        Dataset<Row> avgCityRenamed = avgCityDF.withColumnRenamed("INSEE_C", "Code_INSEE");
        
        // 4. Join the aggregated city dataset with the average rent dataset on Code_INSEE and année
        Dataset<Row> joined = cityAgg.join(
                avgCityRenamed,
                cityAgg.col("Code INSEE").equalTo(avgCityRenamed.col("Code_INSEE"))
                        .and(cityAgg.col("année").equalTo(avgCityRenamed.col("année"))),
                "inner"
        );
        
        // 5. Compute the rental yield using the formula:
        //    rental_yield = ((avg_loypredm2 * total_surface * 12) / total_value) * 100, rounded to 2 decimal places.
        Dataset<Row> yieldDF = joined.withColumn("rental_yield",
                functions.round(functions.expr("((avg_loypredm2 * total_surface * 12) / total_value) * 100"), 2)
        );
        
        // 6. Select the desired columns: LIBGEO, Code_INSEE, année, rental_yield.
        return yieldDF.select(
                avgCityRenamed.col("LIBGEO"),
                avgCityRenamed.col("Code_INSEE"),
                cityAgg.col("année"),
                functions.col("rental_yield")
        );
    }
}
