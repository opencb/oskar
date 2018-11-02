package org.opencb.oskar.spark.variant.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction2;

import java.util.List;

/**
 * Created on 04/09/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileFunction
        extends AbstractFunction2<Object, String, Row>
        implements UDF2<Object, String, Row> {

    @Override
    public Row call(Object o, String fileId) {
        if (o instanceof Row) {
            Row study = (Row) o;
            return getFile(study, fileId);
        } else if (o instanceof WrappedArray) {
            WrappedArray array = (WrappedArray) o;
            for (int i = 0; i < array.size(); i++) {
                Row study = (Row) array.apply(i);
                Row file = getFile(study, fileId);
                if (file != null) {
                    return file;
                }
            }
        } else {
            throw new IllegalArgumentException("");
        }
        return null;
    }

    private Row getFile(Row study, String fileId) {
        List<Row> files = study.getList(study.fieldIndex("files"));
        for (Row file : files) {
            if (fileId.equals(file.getString(file.fieldIndex("fileId")))) {
                return file;
            }
        }
        return null;
    }

    @Override
    public Row apply(Object o, String fileId) {
        return call(o, fileId);
    }
}
