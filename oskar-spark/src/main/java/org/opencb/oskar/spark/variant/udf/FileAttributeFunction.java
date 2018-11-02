package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF3;
import scala.runtime.AbstractFunction3;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileAttributeFunction
        extends AbstractFunction3<Object, String, String, String>
        implements UDF3<Object, String, String, String> {

    @Override
    public String call(Object studies, String fileId, String infoField) {

        Row file = new FileFunction().call(studies, fileId);
        if (file != null) {
            Object infoValue = file.getJavaMap(file.fieldIndex("attributes")).get(infoField);
            return infoValue == null ? null : infoValue.toString();
        }
        return null;

//        Object infoValue = null;
//        Row study = studies.apply(0);
//        List<GenericRowWithSchema> files = study.getList(study.fieldIndex("files"));
//
//        for (GenericRowWithSchema file : files) {
//            if (fileId.equals(file.getAs("fileId"))) {
//                infoValue = file.getJavaMap(file.fieldIndex("attributes")).get(infoField);
//                break;
//            }
//        }
//        return infoValue == null ? null : infoValue.toString();
    }

    @Override
    public String apply(Object studies, String fileId, String infoField) {
        return call(studies, fileId, infoField);
    }
}
