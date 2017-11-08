package au.data61.serene.sereneutils.examples;

import au.data61.serene.sereneutils.core.io.avro.AvroVertex;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroExample {

     public static void main(String[] args) throws IOException {
         AvroVertex avroVertex = AvroVertex.newBuilder()
                 .setId("123")
                 .setLabel("456")
                 .setGraphs(new ArrayList<>())
                 .setData(new HashMap<>())
                 .build();

         Map<CharSequence,Object> data = new HashMap<>();
         data.put("key1", "value1");
         data.put("key2", "value2");
         List<CharSequence> graphs = new ArrayList<>();
         graphs.add("456");
         AvroVertex avroVertex1 = new AvroVertex("456", data, "789", graphs);
         DatumWriter<AvroVertex> vertexDatumWriter = new SpecificDatumWriter<>(AvroVertex.class);
         DataFileWriter<AvroVertex> dataFileWriter = new DataFileWriter<>(vertexDatumWriter);
         dataFileWriter.create(avroVertex.getSchema(), new File("avroVertex.avro"));
         dataFileWriter.append(avroVertex);
         dataFileWriter.append(avroVertex1);
         dataFileWriter.close();

         DatumReader<AvroVertex> vertexDatumReader = new SpecificDatumReader<>(AvroVertex.class);
         DataFileReader<AvroVertex> dataFileReader = new DataFileReader<AvroVertex>(new File("avroVertex.avro"), vertexDatumReader);
         AvroVertex avroVertex2 = null;
         while (dataFileReader.hasNext()) {
             avroVertex2 = dataFileReader.next(avroVertex2);
             System.out.println(avroVertex2);
         }

     }
}
