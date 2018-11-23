package org.opencb.oskar.spark.variant.analysis;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.ArrayContains;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.oskar.spark.commons.OskarException;
import org.opencb.oskar.spark.variant.VariantMetadataManager;
import org.opencb.oskar.spark.variant.udf.VariantUdfManager;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
/**
 * Filter variants that match a given Mode Of Inheritance pattern.
 *
 * Accepted patterns:
 *  - monoallelic, also known as dominant
 *  - biallelic, also known as recessive
 *  - xLinked
 *  - yLinked
 *
 * Created on 21/11/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ModeOfInheritanceTransformer extends AbstractTransformer {

    protected static final String MONOALLELIC = "monoallelic";
    protected static final String BIALLELIC = "biallelic";
    protected static final String X_LINKED = "xLinked";
    protected static final String Y_LINKED = "yLinked";

    private final Param<String> studyIdParam;
    private final Param<String> familyParam;
    private final Param<String> modeOfInheritanceParam;
    private final Param<String> phenotypeParam;
    private final Param<Boolean> incompletePenetranceParam;
    private final Param<Boolean> missingAsReferenceParam;

    public ModeOfInheritanceTransformer() {
        this(null);
    }

    public ModeOfInheritanceTransformer(String modeOfInheritance, String family, String phenotype) {
        this(null);
        setModeOfInheritance(modeOfInheritance);
        setFamily(family);
        setPhenotype(phenotype);
    }

    public ModeOfInheritanceTransformer(String uid) {
        super(uid);
        studyIdParam = new Param<>(this, "studyId", "Id of the study of the family.");
        familyParam = new Param<>(this, "family", "Select family to apply the filter");
        modeOfInheritanceParam = new Param<>(this, "modeOfInheritance", "Filter by mode of inheritance from a given family. "
                + "Accepted values: monoallelic (dominant), biallelic (recessive), xLinked, yLinked");
        phenotypeParam = new Param<>(this, "phenotype", "Specify the phenotype to use for the mode  of inheritance");
        incompletePenetranceParam = new Param<>(this, "incompletePenetrance",
                "Allow variants with an incomplete penetrance mode of inheritance");
        missingAsReferenceParam = new Param<>(this, "missingAsReference", "");

        setDefault(studyIdParam, "");
        setDefault(incompletePenetranceParam, false);
        setDefault(missingAsReferenceParam, false);
    }

    public Param<String> studyIdParam() {
        return studyIdParam;
    }

    public ModeOfInheritanceTransformer setStudyId(String study) {
        set(studyIdParam, study);
        return this;
    }

    public Param<String> familyParam() {
        return familyParam;
    }

    public ModeOfInheritanceTransformer setFamily(String family) {
        set(familyParam, family);
        return this;
    }

    public Param<String> modeOfInheritanceParam() {
        return modeOfInheritanceParam;
    }

    public ModeOfInheritanceTransformer setModeOfInheritance(String modeOfInheritance) {
        set(modeOfInheritanceParam, modeOfInheritance);
        return this;
    }

    public Param<String> phenotypeParam() {
        return phenotypeParam;
    }

    public ModeOfInheritanceTransformer setPhenotype(String phenotype) {
        set(phenotypeParam, phenotype);
        return this;
    }

    public Param<Boolean> incompletePenetranceParam() {
        return incompletePenetranceParam;
    }

    public ModeOfInheritanceTransformer setIncompletePenetrance(boolean incompletePenetrance) {
        set(incompletePenetranceParam, incompletePenetrance);
        return this;
    }

    public Param<Boolean> missingAsReferenceParam() {
        return missingAsReferenceParam;
    }

    public ModeOfInheritanceTransformer setMissingAsReference(boolean missingAsReference) {
        set(missingAsReferenceParam, missingAsReference);
        return this;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> df = (Dataset<Row>) dataset;

        VariantMetadataManager vmm = new VariantMetadataManager();
        String family = getOrDefault(familyParam);
        String moi = getOrDefault(modeOfInheritanceParam);
        String phenotype = getOrDefault(phenotypeParam);
        String studyId = getOrDefault(studyIdParam);
        Boolean incompletePenetrance = getOrDefault(incompletePenetranceParam);
        Boolean missingAsReference = getOrDefault(missingAsReferenceParam);

        Map<String, List<String>> samplesMap = vmm.samples(df);
        boolean multiStudy = samplesMap.size() > 1;
        if (StringUtils.isEmpty(studyId)) {
            if (multiStudy) {
                throw new IllegalArgumentException("Missing study. Multiple studies found: " + samplesMap.keySet());
            }
            studyId = samplesMap.keySet().iterator().next();
        }

        Pedigree pedigree = vmm.pedigree(df, studyId, family);

        Map<String, List<String>> gtsMap;

        String moiSimple = moi.toLowerCase().replace("_", "");
        switch (moiSimple) {
            case MONOALLELIC:
            case "dominant":
                gtsMap = ModeOfInheritance.dominant(pedigree, new Phenotype(phenotype, phenotype, null), incompletePenetrance);
                break;
            case BIALLELIC:
            case "recessive":
                gtsMap = ModeOfInheritance.recessive(pedigree, new Phenotype(phenotype, phenotype, null), incompletePenetrance);
                break;
            case X_LINKED:
                gtsMap = ModeOfInheritance.xLinked(pedigree, new Phenotype(phenotype, phenotype, null), incompletePenetrance);
                break;
            case Y_LINKED:
                gtsMap = ModeOfInheritance.yLinked(pedigree, new Phenotype(phenotype, phenotype, null));
                break;
            default:
                throw new IllegalArgumentException();
        }

        List<String> samples;
        try {
            samples = vmm.samples(df, studyId);
        } catch (OskarException e) {
            throw new IllegalArgumentException(e);
        }

        for (Map.Entry<String, List<String>> entry : gtsMap.entrySet()) {
            String sample = entry.getKey();
            List<String> gts = entry.getValue();
            int idx = samples.indexOf(sample);

            if (missingAsReference) {
                if (gts.contains("0/0")) {
                    gts.add("./.");
                }
            }

            Column sampleGt;
            if (multiStudy) {
                sampleGt = VariantUdfManager.study("studies", studyId).getField("samplesData").apply(idx).apply(0);
            } else {
                sampleGt = col("studies").apply(0).getField("samplesData").apply(idx).apply(0);
            }

            if (gts.size() == 1) {
                df = df.filter(sampleGt.equalTo(gts.get(0)));
            } else {
                // Do not use "org.apache.spark.sql.functions.array_contains", as it forces the second element to be a literal
                df = df.filter(new Column(new ArrayContains(
                        lit(gts.toArray(new String[0])).expr(),
                        sampleGt.expr())));
            }
        }

        return df;
    }
}
