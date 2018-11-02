package org.opencb.oskar.spark.variant.ml;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.util.Identifiable$;
import org.apache.spark.sql.types.StructType;

/**
 * Created on 27/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractTransformer extends Transformer {

    private String uid;

    public AbstractTransformer() {
        this.uid = null;
    }

    public AbstractTransformer(String uid) {
        this.uid = uid;
    }

    @Override
    public String uid() {
        return getUid();
    }

    private String getUid() {
        if (uid == null) {
            uid = Identifiable$.MODULE$.randomUID(this.getClass().getSimpleName());
        }
        return uid;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        return defaultCopy(extra);
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return schema;
    }
}
