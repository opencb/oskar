package org.opencb.oskar.spark.variant.transformers;

import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.core.Region;
import org.opencb.oskar.spark.commons.converters.DataTypeUtils;

import static org.apache.spark.sql.functions.*;

/**
 * Created on 27/09/18.
 *
 * Estimate sex of the individuals calculating the inbreeding coefficients F on the chromosome X.
 *
 * By default, excludes pseudoautosomal regions.
 *
 * With an F smaller than a lowerThreshold (0.2 by default) imputes female sex. and values larger than an upperThreshold
 * (0.8 by default) imputes male sex.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ImputeSexTransformer extends InbreedingCoefficientTransformer {

    private final Param<Float> lowerThresholdParam;
    private final Param<Float> upperThresholdParam;
    private final Param<String> chromosomeXParam;
    private final Param<Boolean> includePseudoautosomalRegionsParam;
    private final Param<String> par1chrXParam;
    private final Param<String> par2chrXParam;
//    private final Param<String> par1chrYParam;
//    private final Param<String> par2chrYParam;


    public ImputeSexTransformer() {
        this(null);
    }

    public ImputeSexTransformer(String uid) {
        super(uid);

        lowerThresholdParam = new Param<>(this, "lowerThreshold", "");
        upperThresholdParam = new Param<>(this, "upperThreshold", "");
        chromosomeXParam = new Param<>(this, "chromosomeX", "");
        includePseudoautosomalRegionsParam = new Param<>(this, "includePseudoautosomalRegions", "");
        par1chrXParam = new Param<>(this, "par1chrX", "");
//        par1chrYParam = new Param<>(this, "par1chrY", "");
        par2chrXParam = new Param<>(this, "par2chrX", "");
//        par2chrYParam = new Param<>(this, "par2chrY", "");

        setDefault(lowerThresholdParam, 0.2F);
        setDefault(upperThresholdParam, 0.8F);
        setDefault(chromosomeXParam, "X");
        setDefault(includePseudoautosomalRegionsParam, false);
        setDefault(par1chrXParam, "X:60001-2699520");
//        setDefault(par1chrYParam, "Y:10001-2649520");
        setDefault(par2chrXParam, "X:154931044-155260560");
//        setDefault(par2chrYParam, "Y:59034050-59363566");
    }

    public Param<Float> lowerThresholdParam() {
        return lowerThresholdParam;
    }

    public Param<Float> upperThresholdParam() {
        return upperThresholdParam;
    }


    public Param<String> chromosomeXParam() {
        return chromosomeXParam;
    }

    public Param<Boolean> includePseudoautosomalRegionsParam() {
        return includePseudoautosomalRegionsParam;
    }

    public Param<String> par1chrXParam() {
        return par1chrXParam;
    }

    public Param<String> par2chrXParam() {
        return par2chrXParam;
    }

//    public Param<String> par1chrYParam() {
//        return par1chrYParam;
//    }

//    public Param<String> par2chrYParam() {
//        return par2chrYParam;
//    }


    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {


        // Filter the dataset to variants on the X chromosome
        String chrX = getOrDefault(chromosomeXParam);

        if (getOrDefault(includePseudoautosomalRegionsParam)) {
            // Include whole chromosome X
            dataset = dataset.where(col("chromosome").equalTo(lit(chrX)));
        } else {

            Region par1chrX = new Region(getOrDefault(par1chrXParam));
            Region par2chrX = new Region(getOrDefault(par2chrXParam));

            // Remove variants in the pseudoautosomal region
            dataset = dataset.where(col("chromosome").equalTo(chrX)
                    .and(
                            col("start").lt(par1chrX.getStart())
                                    .or(col("start").between(par1chrX.getEnd(), par2chrX.getStart()))
                                    .or(col("start").gt(par2chrX.getEnd()))
                    )
            );
        }

        Float lowerThreshold = getOrDefault(lowerThresholdParam);
        Float upperThreshold = getOrDefault(upperThresholdParam);
        return super.transform(dataset)
                .withColumn("ImputedSex", when(col("F").lt(lowerThreshold), "FEMALE")
                        .when(col("F").gt(upperThreshold), "MALE")
                        .otherwise("UNDETERMINED"));

    }

    @Override
    public StructType transformSchema(StructType schema) {
        StructType structType = super.transformSchema(schema);

        return DataTypeUtils.addField(structType, DataTypes.createStructField("ImputedSex", DataTypes.StringType, false));
    }
}
