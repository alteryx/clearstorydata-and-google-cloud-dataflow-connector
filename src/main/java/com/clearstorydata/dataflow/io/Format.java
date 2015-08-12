/*
 * Copyright (c) ClearStory Data, Inc. All Rights Reserved.
 *
 * Please see the COPYRIGHT file in the root of this repository for more details.
 */

package com.clearstorydata.dataflow.io;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ianlcsd on 6/15/15.
 */
public abstract class Format<T> {

    /**
     * Encoder takes a value of type T and encode into string representation.
     *
     * <p> Encoder is used by CsdWriter
     *
     * @param <T> the type which the encoder will encode
     */
    public interface Encoder<T> {
        String encode(T value);
    }

    /**
     * RowReader allows to iterate and close at the end.
     *
     * <p> RowReader is used by CsdReader
     *
     * @param <T> the type which the RowReader reads
     */
    public interface RowReader<T>  extends Iterator<T>, Closeable {}

    public abstract RowReader<T> createReader(final InputStream is);

    public abstract Encoder<T> getEncoder();

    public static Format<List<String>> ofCsv() {
        return new Csv();
    }

    public static class Csv extends Format<List<String>>  implements Serializable {
        private static final long serialVersionUID = 0;

        @Override
        public RowReader<List<String>> createReader(final InputStream is) {
            return new RowReader<List<String>>() {
                private List<String> current;
                private final BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(is, Charset.forName("UTF-8")));

                @Override
                public boolean hasNext() {
                    String line = null;
                    try {
                        line = bufferedReader.readLine();
                        if (line != null) {
                            current = Arrays.asList(line.split(","));
                        } else {
                            current = null;
                        }
                        return current != null;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public List<String> next() {
                    return current;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove() is not "
                            + "supported!!");
                }

                @Override
                public void close() throws IOException {
                    bufferedReader.close();
                }
            };
        }


        @Override
        public Encoder<List<String>> getEncoder() {
            return new Encoder<List<String>>() {

                @Override
                public String encode(List<String> value) {
                    StringBuffer sb = new StringBuffer();
                    boolean start = true;
                    for (String str : value) {
                        if (str == null) {
                            str = "";
                        }

                        if (start) {
                            sb.append(str);
                            start = false;
                        } else {
                            sb.append(",").append(str);
                        }
                    }
                    return sb.append("\n").toString();
                }
            };
        }
    }

}
