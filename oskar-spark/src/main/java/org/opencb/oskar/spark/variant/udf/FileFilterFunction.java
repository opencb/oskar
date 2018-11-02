package org.opencb.oskar.spark.variant.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF2;
import org.opencb.biodata.models.variant.StudyEntry;
import scala.runtime.AbstractFunction2;

import java.util.Arrays;
import java.util.List;

/**
 * Created on 07/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileFilterFunction extends AbstractFunction2<Object, String, List<String>> implements UDF2<Object, String, List<String>> {

    @Override
    public List<String> call(Object o, String fileId) {
        String filter = new FileAttributeFunction().call(o, fileId, StudyEntry.FILTER);
        if (StringUtils.isNotEmpty(filter)) {
            return Arrays.asList(filter.split(";"));
        } else {
            return null;
        }
    }

    @Override
    public List<String> apply(Object o, String file) {
        return call(o, file);
    }

}
