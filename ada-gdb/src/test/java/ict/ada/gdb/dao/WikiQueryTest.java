package ict.ada.gdb.dao;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.util.Hex;
import ict.ada.gdb.common.AdaModeConfig.GDBMode;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.RelQuerySpec.RelQuerySpecBuilder;
import ict.ada.gdb.service.AdaGdbService;

import java.io.File;
import java.util.Scanner;

public class WikiQueryTest {
  private AdaGdbService gdb = new AdaGdbService(GDBMode.QUERY);

  private static final RelationType GONGXIAN = RelationType.getType("共现");

  private void testNone(String nodeA) throws Exception {
    // no cache
    RelQuerySpec specA1 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(true).build();
    // cache
    RelQuerySpec specA2 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(false).build();
    RelationGraph gA1 = gdb.queryRelationGraph(specA1);
    RelationGraph gA2 = gdb.queryRelationGraph(specA2);
    assertRelationGraph(gA1, gA2);
  }

  private void testRelType(String nodeA) throws Exception {
    // no cache
    RelQuerySpec specA1 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(true).relType(GONGXIAN).build();
    // cache
    RelQuerySpec specA2 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(false).relType(GONGXIAN).build();
    RelationGraph gA1 = gdb.queryRelationGraph(specA1);
    RelationGraph gA2 = gdb.queryRelationGraph(specA2);
    assertRelationGraph(gA1, gA2);
  }

  private void testNodeType(String nodeA) throws Exception {
    // no cache
    RelQuerySpec spec1 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(true).attribute(Attribute.LOCATION).build();
    // cache
    RelQuerySpec spec2 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(false).attribute(Attribute.LOCATION).build();
    RelationGraph g1 = gdb.queryRelationGraph(spec1);
    RelationGraph g2 = gdb.queryRelationGraph(spec2);
    assertRelationGraph(g1, g2);

  }

  private void testRelTypeAndNodeType(String nodeA) throws Exception {
    // RelType + NodeType
    // no cache
    RelQuerySpec specB1 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(true).relType(GONGXIAN).attribute(Attribute.LOCATION).build();
    // cache
    RelQuerySpec specB2 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(nodeA)))//
        .isCritical(false).relType(GONGXIAN).attribute(Attribute.LOCATION).build();
    RelationGraph gB1 = gdb.queryRelationGraph(specB1);
    RelationGraph gB2 = gdb.queryRelationGraph(specB2);
    assertRelationGraph(gB1, gB2);

  }

  private static final String LOG_PREFIX = "[ASSERT] ";

  private static void assertRelationGraph(RelationGraph a, RelationGraph b) {
    int sizeA = a.getCenterEdges().size();
    int sizeB = b.getCenterEdges().size();
    if (sizeA == sizeB) {
      System.out.println(LOG_PREFIX + "OK size=" + sizeA);
    } else {
      throw new RuntimeException(LOG_PREFIX + "FALSE. sizeA=" + sizeA + " sizeB=" + sizeB);
    }
  }

  private static void testWithFileData(String path) throws Exception {
    WikiQueryTest tester = new WikiQueryTest();
    Scanner scanner = new Scanner(new File(path));
    while (scanner.hasNext()) {
      String node = scanner.nextLine();
      tester.testNone(node);
      tester.testNodeType(node);
      tester.testRelType(node);
      tester.testRelTypeAndNodeType(node);
    }

  }

  private static void testStaticNodes() throws Exception {
    WikiQueryTest test = new WikiQueryTest();
    String[] nodes = {//
    "01324377bbdb5ff333304ebfb17ab365d26b",// wiki likaifu
        "01325675e69c8c01d124fc5adcefa1a417a8",// wiki yaochen
        "0132a1d6f7efe81868b05ec2f22960037171",// wiki linshuhao
    };
    for (String node : nodes) {
      test.testNone(node);
      test.testNodeType(node);
      test.testRelType(node);
      test.testRelTypeAndNodeType(node);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("Use hard-coded nodes.");
      testStaticNodes();
    } else {
      System.out.println("Use nodes in file: " + args[0]);
      testWithFileData(args[0]);
    }
    System.exit(0);
  }
}
