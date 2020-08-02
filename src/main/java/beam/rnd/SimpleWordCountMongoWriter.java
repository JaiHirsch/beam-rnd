 /*
        Copyright 2020 Jai Hirsch

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.
*/

package beam.rnd;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.bson.Document;

import java.util.Arrays;

public class SimpleWordCountMongoWriter {

    public static void main(String[] args) {

        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from("./src/test/resources/test.csv"))
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split(","))))
                .apply(Count.<String>perElement())
                .apply("FormatResults", MapElements
                        .into(TypeDescriptor.of(Document.class))
                        .via((KV<String, Long> wordCount) -> new Document(wordCount.getKey(),wordCount.getValue())))
                .apply("MongoDB write", MongoDbIO.write()
                .withUri("mongodb://localhost:27017")
                .withDatabase("test")
                .withCollection("wordcount"));
        p.run().waitUntilFinish();

    }
}
