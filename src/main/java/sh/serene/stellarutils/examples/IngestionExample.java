package sh.serene.stellarutils.examples;

import sh.serene.stellarutils.entities.Properties;
import sh.serene.stellarutils.graph.api.StellarBackEndFactory;
import sh.serene.stellarutils.graph.api.StellarGraph;
import sh.serene.stellarutils.graph.api.StellarGraphBuffer;
import sh.serene.stellarutils.graph.impl.local.LocalBackEndFactory;

public class IngestionExample {

    public static void main(String[] args) {
        StellarBackEndFactory backEndFactory = new LocalBackEndFactory();
        StellarGraphBuffer graphBuffer = backEndFactory.createGraph("small-example", null);

        // persons
        int nPersons = 6;
        String[] names = {"kevin", "filippo", "alex", "chao", "yuriy", "hooman"};
        for (int i = 0; i < nPersons; i++) {
            Properties props = Properties.create();
            props.add("name", names[i]);
            props.add("age", i*7);
            graphBuffer.addVertex(names[i],"person", props.getMap());
        }

        // dogs
        int nDogs = 6;
        String[] dogNames = {"pikachu", "marley", "odie", "scooby", "snoopy", "buddy"};
        for (int i = 0; i < nDogs; i++) {
            Properties props = Properties.create();
            props.add("name", dogNames[i]);
            props.add("age", i);
            graphBuffer.addVertex(dogNames[i], "dog", props.getMap());
        }

        // owners
        for (int i = 0; i < nDogs; i++) {
            // assign each dog to some owner
            int j = (int) dogNames[i].charAt(0) % nPersons;
            graphBuffer.addEdge(names[j] + "[owns]" + dogNames[i], names[j], dogNames[i], "owns", null);
        }

        StellarGraph graph = graphBuffer.toGraph();
        graph.toCollection().write().json("dogs.epgm");

    }
}
