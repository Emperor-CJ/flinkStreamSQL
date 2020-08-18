package com.dtstack.flink.sql.sink.dingding;

import java.io.*;

public class ObjClonerSeiz implements Serializable {
    public static <T> T CloneObj(T obj) {
        T retobj = null;
        try {
            //写入流中
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            //从流中读取
            ObjectInputStream ios = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            retobj = (T) ios.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retobj;
    }

}
