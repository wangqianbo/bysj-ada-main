/**
 * 
 */
package ict.ada.graphcached.test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * @author forhappy
 * 
 */
public class HelloWorld {
  private static final String DB_PATH = "data/";

  String myString;
  GraphDatabaseService graphDb;
  Node myFirstNode;
  Node mySecondNode;
  Relationship myRelationship;

  private static enum RelTypes implements RelationshipType {
    KNOWS
  }

  public static void main(final String[] args) {

    HelloWorld myNeoInstance = new HelloWorld();
    myNeoInstance.createDb();
    myNeoInstance.removeData();
    myNeoInstance.shutDown();

  }

  void createDb() {
    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);

    Transaction tx = graphDb.beginTx();
    try {
      myFirstNode = graphDb.createNode();
      myFirstNode.setProperty("name", "forhappy");
      mySecondNode = graphDb.createNode();
      mySecondNode.setProperty("name", "yppahrof");
      
      myRelationship = myFirstNode.createRelationshipTo(mySecondNode, RelTypes.KNOWS);
      myRelationship.setProperty("type", "knows");

      myString = (myFirstNode.getProperty("name").toString()) + " "
          + (myRelationship.getProperty("type").toString()) + " "
          + (mySecondNode.getProperty("name").toString());
      System.out.println(myString);

      tx.success();
    } finally {
      tx.finish();
    }
  }

  void removeData() {
    Transaction tx = graphDb.beginTx();
    try {
      myFirstNode.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING).delete();
      System.out.println("Removing nodes...");
      myFirstNode.delete();
      mySecondNode.delete();
      tx.success();
    } finally {
      tx.finish();
    }
  }

  void shutDown() {
    graphDb.shutdown();
    System.out.println("graphDB shut down.");
  }

}
