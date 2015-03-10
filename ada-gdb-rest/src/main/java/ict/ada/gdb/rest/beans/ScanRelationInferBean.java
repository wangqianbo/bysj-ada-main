package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.util.PojoMapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class ScanRelationInferBean {
  private String errorCode;
  private List<Rule> rules;
  private int count;

  public static HashMap<String, List<String>> openiotoGDB = new HashMap<String, List<String>>(); // 最好用配置文件来做
  static {
    openiotoGDB.put("organization", Arrays.asList("org"));
    // openiotoGDB.put("person", Arrays.asList("web", "weibo"));
    openiotoGDB.put("any_person", Arrays.asList("any_person"));
    openiotoGDB.put("person", Arrays.asList("person"));
    openiotoGDB.put("location", Arrays.asList("location"));

  }
  public static HashMap<String, String> GDBtoopenio = new HashMap<String, String>();
  static {
    GDBtoopenio.put("org", "organization");
    GDBtoopenio.put("web", "person");
    GDBtoopenio.put("weibo", "person");
    GDBtoopenio.put("location", "location");
    GDBtoopenio.put("scholar", "person");
    GDBtoopenio.put("bbs", "person");
    GDBtoopenio.put("twitter", "person");
    GDBtoopenio.put("facebook", "person");
    GDBtoopenio.put("googleplus", "person");
    GDBtoopenio.put("linkedin", "person");
    GDBtoopenio.put("baidu_baike", "person");
    GDBtoopenio.put("hudong_baike", "person");
    GDBtoopenio.put("wiki_baike", "person");
    GDBtoopenio.put("law", "law");
    GDBtoopenio.put("weapon", "weapon");
    GDBtoopenio.put("email", "email");
    GDBtoopenio.put("im", "im");
    GDBtoopenio.put("telephone", "telephone");
    GDBtoopenio.put("time", "time");
    GDBtoopenio.put("event", "event");
    GDBtoopenio.put("category", "category");
    GDBtoopenio.put("identity", "identity");

  }

  public ScanRelationInferBean() {
    rules = new ArrayList<Rule>();
  }

  public ScanRelationInferBean(ScanRelationInferBean internalbean) {
    rules = new ArrayList<Rule>();
    this.errorCode = internalbean.errorCode;
    for (Rule oldrule : internalbean.rules) {
      this.rules.addAll(oldrule.mapRule());
    }
  }

  public ScanRelationInferBean(ScanRelationInferBean internalbean, String channel) {
    rules = new ArrayList<Rule>();
    this.errorCode = internalbean.errorCode;
    for (Rule oldrule : internalbean.rules) {
      this.rules.addAll(oldrule.mapRule(channel));
    }
  }

  public void addRule(Rule rule) {
    rules.add(rule);
  }

  /**
   * @return the errorCode
   */
  public String getErrorCode() {
    return errorCode;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  /**
   * @param errorCode
   *          the errorCode to set
   */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  /**
   * @return the rules
   */
  public List<Rule> getRules() {
    return rules;
  }

  /**
   * @param rules
   *          the rules to set
   */
  public void setRules(List<Rule> rules) {
    this.rules = rules;
  }

  public static class Rule {
    private String inference_name;
    private String description;
    private String score;
    private List<Regulation> rule;

    private int uid;

    public Rule() {
      rule = new ArrayList<Regulation>();
    }

    public Rule(Rule rule) {
      this.inference_name = rule.inference_name;
      this.description = rule.description;
      this.score = rule.score;
      this.rule = new ArrayList<Regulation>(rule.rule);
    }

    public void addRegulation(Regulation reg) {
      this.rule.add(reg);
    }

    public List<Rule> mapRule() {
      List<Rule> newrule = null;
      List<Rule> tmpnewrule = new ArrayList<Rule>();
      Rule tmpsigrule = new Rule(this);
      tmpsigrule.setRule(new ArrayList<Regulation>());
      tmpnewrule.add(tmpsigrule);
      for (Regulation reg : this.rule) {
        newrule = new ArrayList<Rule>();
        for (Regulation mapreg : tmpsigrule.mapReg(reg)) {
          for (Rule rule : tmpnewrule) {
            Rule newsigrule = new Rule(rule);
            newsigrule.addRegulation(mapreg);
            newrule.add(newsigrule);
          }
        }
        tmpnewrule = new ArrayList<Rule>(newrule);
        // Collections.(tmpnewrule, newrule);
      }
      return newrule;
    }

    public List<Rule> mapRule(String channel) {
      List<Rule> newrule = null;
      List<Rule> tmpnewrule = new ArrayList<Rule>();
      Rule tmpsigrule = new Rule(this);
      tmpsigrule.setRule(new ArrayList<Regulation>());
      tmpnewrule.add(tmpsigrule);
      for (Regulation reg : this.rule) {
        newrule = new ArrayList<Rule>();
        for (Regulation mapreg : tmpsigrule.mapReg(reg, channel)) {
          for (Rule rule : tmpnewrule) {
            Rule newsigrule = new Rule(rule);
            newsigrule.addRegulation(mapreg);
            newrule.add(newsigrule);
          }
        }
        tmpnewrule = new ArrayList<Rule>(newrule);
        // Collections.(tmpnewrule, newrule);
      }
      return newrule;
    }

    private List<Regulation> mapReg(Regulation oldReg) {
      return null;

    }

    private List<Regulation> mapReg(Regulation oldReg, String channel) {
      return null;
    }

    /**
     * @return the inference_name
     */
    public String getInference_name() {
      return inference_name;
    }

    /**
     * @param inference_name
     *          the inference_name to set
     */
    public void setInference_name(String inference_name) {
      this.inference_name = inference_name;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    /**
     * @param description
     *          the description to set
     */
    public void setDescription(String description) {
      this.description = description;
    }

    /**
     * @return the score
     */
    public String getScore() {
      return score;
    }

    /**
     * @param score
     *          the score to set
     */
    public void setScore(String score) {
      this.score = score;
    }

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
    public int getUid() {
      return uid;
    }

    public void setUid(int uid) {
      this.uid = uid;
    }

    /**
     * @return the rule
     */
    public List<Regulation> getRule() {
      return rule;
    }

    /**
     * @param rule
     *          the rule to set
     */
    public void setRule(List<Regulation> rule) {
      this.rule = rule;
    }

    public static class Regulation {
      private List<String> relation;
      private Node startnode;
      private Node endnode;

      /**
       * @return the relation
       */
      public List<String> getRelation() {
        return relation;
      }

      /**
       * @param relation
       *          the relation to set
       */
      public void setRelation(List<String> relation) {
        this.relation = relation;
      }

      public Node getStartnode() {
        return startnode;
      }

      public void setStartnode(Node startnode) {
        this.startnode = startnode;
      }

      public Node getEndnode() {
        return endnode;
      }

      public void setEndnode(Node endnode) {
        this.endnode = endnode;
      }

    }
  }

  public static class Node {
    private List<String> type;
    private String var;

    public List<String> getType() {
      return type;
    }

    public void setType(List<String> type) {
      this.type = type;
    }

    public String getVar() {
      return var;
    }

    public void setVar(String var) {
      this.var = var;
    }

  }

  public static void main(String[] args) throws JsonParseException, IOException {
    FileReader fr = new FileReader(new File("/home/wangqianbo/test/test"));
    ScanRelationInferBean bean = new ScanRelationInferBean(
        (ScanRelationInferBean) PojoMapper.fromJson(fr, ScanRelationInferBean.class), "web");
    System.out.println(PojoMapper.toJson(bean, true));

  }

}