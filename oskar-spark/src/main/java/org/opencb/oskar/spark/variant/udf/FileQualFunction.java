package org.opencb.oskar.spark.variant.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF2;
import org.opencb.biodata.models.variant.StudyEntry;
import scala.runtime.AbstractFunction2;

/**
 * Created on 07/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileQualFunction extends AbstractFunction2<Object, String, Double> implements UDF2<Object, String, Double> {

    @Override
    public Double call(Object o, String fileId) {
        String qual = new FileAttributeFunction().call(o, fileId, StudyEntry.QUAL);
        if (StringUtils.isNotEmpty(qual) && !qual.equals(".")) {
            return Double.parseDouble(qual);
        } else {
            return null;
        }
    }

    @Override
    public Double apply(Object o, String file) {
        return call(o, file);
    }

}
