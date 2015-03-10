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

public class GetRelationInferBean {
  private String errorCode;
  private List<Rule> rules;

  public static HashMap<String, List<String>> openio2GDB = new HashMap<String, List<String>>(); // 最好用配置文件来做
  static {
    openio2GDB.put("organization", Arrays.asList("org"));
    // openiotoGDB.put("person", Arrays.asList("web", "weibo"));
    openio2GDB.put("any_person", Arrays.asList("any_person"));
    openio2GDB.put("person", Arrays.asList("person"));
    openio2GDB.put("location", Arrays.asList("location"));

  }
  public static HashMap<String, String> GDB2openio = new HashMap<String, String>();
  static {
    GDB2openio.put("org", "organization");
    GDB2openio.put("person", "person");
    GDB2openio.put("location", "location");
    GDB2openio.put("law", "law");
    GDB2openio.put("weapon", "weapon");
    GDB2openio.put("email", "email");
    GDB2openio.put("qq", "qq");
    GDB2openio.put("telephone", "telephone");
    GDB2openio.put("time", "time");
    GDB2openio.put("event", "event");
    GDB2openio.put("category", "category");

  }

  public GetRelationInferBean() {
    rules = new ArrayList<Rule>();
  }

  public GetRelationInferBean(GetRelationInferBean internalbean) {
    rules = new ArrayList<Rule>();
    this.errorCode = internalbean.errorCode;
    for (Rule oldrule : internalbean.rules) {
      this.rules.addAll(oldrule.mapRule());
    }
  }

  public GetRelationInferBean(GetRelationInferBean internalbean, String channel) {
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
      List<Regulation> newReg = new ArrayList<Regulation>();
      // Regulation reg= new Regulation();
      // reg.relation=oldReg.relation;
      for (String singlenode : oldReg.node)
        for (String newsinglenode : openio2GDB.get(singlenode)) {
          Regulation reg = new Regulation();
          reg.relation = oldReg.relation;
          reg.node = Arrays.asList(newsinglenode); // 因为在GDB查询中不支持多种点类型查询，因此做单点处理，这只是个临时的方案，//TODO
          newReg.add(reg);
        }
      // node.addAll(GetRelationInferBean.openiotoGDB.get(singlenode)); //是不是需要 去重复呢？
      return newReg;
    }

    private List<Regulation> mapReg(Regulation oldReg, String channel) {
      List<Regulation> newReg = new ArrayList<Regulation>();
      // Regulation reg= new Regulation();
      // reg.relation=oldReg.relation;
      for (String singlenode : oldReg.node)
        if (openio2GDB.get(singlenode) == null) {
          Regulation reg = new Regulation();
          reg.relation = oldReg.relation;
          reg.node = Arrays.asList(singlenode); // 因为在GDB查询中不支持多种点类型查询，因此做单点处理，这只是个临时的方案，//TODO
          newReg.add(reg);
        } else if (openio2GDB.get(singlenode).contains(channel)) {
          Regulation reg = new Regulation();
          reg.relation = oldReg.relation;
          reg.node = Arrays.asList(channel); // 因为在GDB查询中不支持多种点类型查询，因此做单点处理，这只是个临时的方案，//TODO
          newReg.add(reg);
        } else {
          for (String newsinglenode : openio2GDB.get(singlenode)) {
            Regulation reg = new Regulation();
            reg.relation = oldReg.relation;
            reg.node = Arrays.asList(newsinglenode); // 因为在GDB查询中不支持多种点类型查询，因此做单点处理，这只是个临时的方案，//TODO
            newReg.add(reg);
          }
        }
      // node.addAll(GetRelationInferBean.openiotoGDB.get(singlenode)); //是不是需要 去重复呢？
      return newReg;
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
      private List<String> node;

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

      /**
       * @return the node
       */
      public List<String> getNode() {
        return node;
      }

      /**
       * @param node
       *          the node to set
       */
      public void setNode(List<String> node) {
        this.node = node;
      }

    }
  }

  public static void main(String[] args) throws JsonParseException, IOException {
    FileReader fr = new FileReader(new File("/home/wangqianbo/test/test"));
    GetRelationInferBean bean = new GetRelationInferBean(
        (GetRelationInferBean) PojoMapper.fromJson(fr, GetRelationInferBean.class), "web");
    System.out.println(PojoMapper.toJson(bean, true));

  }

}