package simpledb.planner;

import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.parse.*;
import simpledb.server.SimpleDB;

import javax.management.Query;
import java.util.*;

/**
 * The simplest, most naive query planner possible.
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlanner {
   
   /**
    * Creates a query plan as follows.  It first takes
    * the product of all tables and views; it then selects on the predicate;
    * and finally it projects on the field list. 
    */
   private Plan createPlan(QueryData datum, Transaction tx) {
      //Step 1: Create a plan for each mentioned table or view
      List<Plan> plans = new ArrayList<Plan>();
      for (String tblname : datum.tables()) {
         String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
         if (viewdef != null)
            plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
         else
            plans.add(new TablePlan(tblname, tx));
      }

      //Step 2: Create the product of all table plans
      Plan p1 = plans.remove(0);
      for (Plan nextplan : plans)
         p1 = new ProductPlan(p1, nextplan);
      //Step 3: Add a selection plan for the predicate
      p1 = new SelectPlan(p1, datum.pred());

      //Step 4: Project on the field names
      p1 = new ProjectPlan(p1, datum.fields().keySet());

      //Step 5: apply rename plan if needed.
      for (Map.Entry<String,String> fieldNames : datum.fields().entrySet()) {
         if (fieldNames.getValue() != null) {
            p1 = new RenamePlan(p1, fieldNames.getKey(), fieldNames.getValue());
         }
      }

      return p1;
   }

   /**
    * Creates plan based on list of QueryData.
    * If there are multiple QueryData (>1) then it will generate
    * UnionPlan of all subsequent plans.
    * @param data the parsed representation of the query
    * @param tx the calling transaction
    * @return plan of all QueryData
    */
   public Plan createPlan(List<QueryData> data, Transaction tx) {
      QueryData firstDatum = data.remove(0);
      Plan plan = createPlan(firstDatum, tx);
      // Create plans for rest of the data (if they exist).
      for (QueryData datum : data) {
         Plan additionalPlan = createPlan(datum, tx);
         plan = new UnionPlan(plan, additionalPlan);
      }

      return plan;
   }
}
