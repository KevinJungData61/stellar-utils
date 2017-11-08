package au.data61.serene.sereneutils.examples;

import au.data61.serene.sereneutils.core.io.avro.AvroVertex;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetExample {

    public static void main(String[] args) throws IOException {
        Map<CharSequence,Object> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");
        List<CharSequence> graphs = new ArrayList<>();
        graphs.add("456");
        AvroVertex avroVertex = new AvroVertex("456", data, "789", graphs);
        ParquetWriter<Object> avroParquetWriter = AvroParquetWriter.builder(new Path("./out"))
                .withSchema(avroVertex.getSchema()).build();
        avroParquetWriter.write(avroVertex);
        avroParquetWriter.close();

        ParquetReader<GenericRecord> avroParquetReader = AvroParquetReader.builder(new Path("./out"))
                .build();
        GenericRecord nextRecord = avroParquetReader.read();
        System.out.println(nextRecord);
        avroParquetReader.close();

    }

}
