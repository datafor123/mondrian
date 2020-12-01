/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ParameterExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Parameter;
import mondrian.olap.Query;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapEvaluator;

/**
 * Definition of the <code>Subset</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 * @author Gcy
 *flattened:
 *0(default,parent included count);
 *1(parent excluded count,add all parent on bottom,check nonempty use nextStart) 
 *2(parent excluded count,all parent adds to the end,saiku use flattend to hide);
 *3(parent excluded count,add relational parent on each top of level,relational parent means there is an index,all the members'index bigger than the index  are [All] and all the member's index smaller than the index  are not parent )
 *4(parent excluded count,acconding to subset result to add relational parent on each top of level,saiku use flat/mix to show as sum row);
 *5(parent excluded count,add all parent on bottom) 
 */
class SubsetFunDef extends FunDefBase {
    private final int ctag = 4;
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Subset",
            "Subset(<Set>, <Start>[, <Count>][, <Flattened>][, <nextStart>])",
            "Returns a subset of elements from a set.",
            new String[] {"fxxn", "fxxnn", "fxxnnn", "fxxnnnn"},
            SubsetFunDef.class);
    
    public SubsetFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
            compiler.compileList(call.getArg(0));
        final IntegerCalc startCalc =
            compiler.compileInteger(call.getArg(1));
        final IntegerCalc countCalc =
            call.getArgCount() > 2
            ? compiler.compileInteger(call.getArg(2))
            : null;
        
        final IntegerCalc flattenedCalc =
                    call.getArgCount() > 3
                    ? compiler.compileInteger(call.getArg(3))
                    : null;
        final IntegerCalc nextStartCalc =
                            call.getArgCount() > 4
                            ? compiler.compileInteger(call.getArg(4))
                            : null;                  
        return new AbstractListCalc(
            call, new Calc[] {listCalc, startCalc, countCalc})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                final int savepoint = evaluator.savepoint();
                try {
                	int start = startCalc.evaluateInteger(evaluator);
                	if(start<0){start=0;}
                	int count =10;
                    int end;
                    if (countCalc != null) {
                         count = countCalc.evaluateInteger(evaluator);
                         end = start + count;
                     } else {
                         end = -1;
                     }
                    evaluator.setAttribute("pageSize", count);
                    int flattened=0;
                    if (flattenedCalc != null) {
                       flattened = flattenedCalc.evaluateInteger(evaluator);
                    } else {
                    	flattened=0;
                    }
                    int nextStart=0;
                    if (nextStartCalc != null) {
                       nextStart = nextStartCalc.evaluateInteger(evaluator);
                    } else {
                    	nextStart=-1;
                    }
                	System.out.println("nonEmpty58:"+evaluator.isNonEmpty());
                	TupleList list;
                	if(flattened==1){
						list = listCalc.evaluateList(evaluator);
                	}else{
                		 evaluator.setNonEmpty(false);
                		 list = listCalc.evaluateList(evaluator);
                	}
                    if(flattened!=0&&flattened!=1&&flattened!=5){
                    if(evaluator instanceof RolapEvaluator){
                    	((RolapEvaluator) evaluator).setFlattened(flattened);
                    	//list=((RolapEvaluator)evaluator).gainFilteredList(list);
                    	((RolapEvaluator) evaluator).setFlattened(0);
                    }
                    }
                    int size=list.size();
                    evaluator.setAttribute("pageNum", size);
                    evaluator.setAttribute("nextStart", start+count);
                    if (end<=0) {
                        end = size;
                    }
                    if (end > size) {
                        end = size;
                    }
                    
					if (start >= end) {
						list = TupleCollections.emptyList(list.getArity());
					} else if (start == 0 && end == list.size()) {
						if (flattened == 1) {
							list = nonEmptyList(start, count, nextStart,evaluator, list,call);
						}
					} else {
						if (flattened == 1) {
							list = nonEmptyList(start, count, nextStart, evaluator, list,call);
						} else {
							list = list.subList(start, end);
						}
					}
					evaluator.setAttribute("pageRec", new Long(list.size()));
                    if (flattened == 2) {
						
					} else if (flattened == 1) {
						/*if (evaluator instanceof RolapEvaluator) {
							list.addAll(((RolapEvaluator) evaluator).getExtraTupleList());
						}*/
					}else if (flattened == 5) {
						/*if (evaluator instanceof RolapEvaluator) {
							list.addAll(((RolapEvaluator) evaluator).getExtraTupleList());
						}*/
					}
                     //gcy effect page end
                    return list;
                     
                } finally {
                    evaluator.restore(savepoint);
                }
            }
        };
    }
    /**
     * This is the entry point to the crossjoin non-empty optimizer code.
     *
     * <p>What one wants to determine is for each individual Member of the input
     * parameter list, a 'List-Member', whether across a slice there is any
     * data.
     *
     * <p>But what data?
     *
     * <p>For Members other than those in the list, the 'non-List-Members',
     * one wants to consider
     * all data across the scope of these other Members. For instance, if
     * Time is not a List-Member, then one wants to consider data
     * across All Time. Or, if Customer is not a List-Member, then
     * look at data across All Customers. The theory here, is if there
     * is no data for a particular Member of the list where all other
     * Members not part of the list are span their complete hierarchy, then
     * there is certainly no data for Members of that Hierarchy at a
     * more specific Level (more on this below).
     *
     * <p>When a Member that is a non-List-Member is part of a Hierarchy
     * that has an
     * All Member (hasAll="true"), then its very easy to make sure that
     * the All Member is used during the optimization.
     * If a non-List-Member is part of a Hierarchy that does not have
     * an All Member, then one must, in fact, iterate over all top-level
     * Members of the Hierarchy!!! - otherwise a List-Member might
     * be excluded because the optimization code was not looking everywhere.
     *
     * <p>Concerning default Members for those Hierarchies for the
     * non-List-Members, ignore them. What is wanted is either the
     * All Member or one must iterate across all top-level Members, what
     * happens to be the default Member of the Hierarchy is of no relevant.
     *
     * <p>The Measures Hierarchy has special considerations. First, there is
     * no All Measure. But, certainly one need only involve Measures
     * that are actually in the query... yes and no. For Calculated Measures
     * one must also get all of the non-Calculated Measures that make up
     * each Calculated Measure. Thus, one ends up iterating across all
     * Calculated and non-Calculated Measures that are explicitly
     * mentioned in the query as well as all Calculated and non-Calculated
     * Measures that are used to define the Calculated Measures in
     * the query. Why all of these? because this represents the total
     * scope of possible Measures that might yield a non-null value
     * for the List-Members and that is what we what to find. It might
     * be a super set, but thats ok; we just do not want to miss anything.
     *
     * <p>For other Members, the default Member is used, but for Measures one
     * should look for that data for all Measures associated with the query, not
     * just one Measure. For a dense dataset this may not be a problem or even
     * apparent, but for a sparse dataset, the first Measure may, in fact, have
     * not data but other Measures associated with the query might.
     * Hence, the solution here is to identify all Measures associated with the
     * query and then for each Member of the list, determine if there is any
     * data iterating across all Measures until non-null data is found or the
     * end of the Measures is reached.
     *
     * <p>This is a non-optimistic implementation. This means that an
     * element of the input parameter List is only not included in the
     * returned result List if for no combination of Measures, non-All
     * Members (for Hierarchies that have no All Members) and evaluator
     * default Members did the element evaluate to non-null.
     *
     * @param evaluator Evaluator
     *
     * @param list      List of members or tuples
     *
     * @param call      Calling ResolvedFunCall used to determine what Measures
     *                  to use
     *
     * @return List of elements from the input parameter list that have
     * evaluated to non-null.
     */
    protected TupleList nonEmptyList(int start,int count,int nextStart,
        Evaluator evaluator,
        TupleList list,
        ResolvedFunCall call)
    {
        if (list.isEmpty()) {
            return list;
        }

        TupleList result =
            TupleCollections.createList(
                list.getArity(), (list.size() + 2) >> 1);

        // Get all of the Measures
        final Query query = evaluator.getQuery();

        final String measureSetKey = "MEASURE_SET-" + ctag;
        Set<Member> measureSet =
            Util.cast((Set) query.getEvalCache(measureSetKey));
        // If not in query cache, then create and place into cache.
        // This information is used for each iteration so it makes
        // sense to create and cache it.
        if (measureSet == null) {
            measureSet = new HashSet<Member>();
            Set<Member> queryMeasureSet = query.getMeasuresMembers();
            MeasureVisitor visitor = new MeasureVisitor(measureSet, call);
            for (Member m : queryMeasureSet) {
                if (m.isCalculated()) {
                    Exp exp = m.getExpression();
                    exp.accept(visitor);
                } else {
                    measureSet.add(m);
                }
            }

            Formula[] formula = query.getFormulas();
            if (formula != null) {
                for (Formula f : formula) {
                    f.accept(visitor);
                }
            }

            query.putEvalCache(measureSetKey, measureSet);
        }

        final String allMemberListKey = "ALL_MEMBER_LIST-" + ctag;
        List<Member> allMemberList =
            Util.cast((List) query.getEvalCache(allMemberListKey));

        final String nonAllMembersKey = "NON_ALL_MEMBERS-" + ctag;
        Member[][] nonAllMembers =
            (Member[][]) query.getEvalCache(nonAllMembersKey);
        if (nonAllMembers == null) {
            //
            // Get all of the All Members and those Hierarchies that
            // do not have All Members.
            //
            Member[] evalMembers = evaluator.getMembers().clone();

            List<Member> listMembers = list.get(0);

            // Remove listMembers from evalMembers and independentSlicerMembers
            for (Member lm : listMembers) {
                Hierarchy h = lm.getHierarchy();
                for (int i = 0; i < evalMembers.length; i++) {
                    Member em = evalMembers[i];
                    if ((em != null) && h.equals(em.getHierarchy())) {
                        evalMembers[i] = null;
                    }
                }
            }

            List<? extends Member> slicerMembers = null;
            if (evaluator instanceof RolapEvaluator) {
                RolapEvaluator rev = (RolapEvaluator) evaluator;
                slicerMembers = rev.getSlicerMembers();
            }
            // Iterate the list of slicer members, grouping them by hierarchy
            Map<Hierarchy, Set<Member>> mapOfSlicerMembers =
                new HashMap<Hierarchy, Set<Member>>();
            if (slicerMembers != null) {
                for (Member slicerMember : slicerMembers) {
                    Hierarchy hierarchy = slicerMember.getHierarchy();
                    if (!mapOfSlicerMembers.containsKey(hierarchy)) {
                        mapOfSlicerMembers.put(
                            hierarchy,
                            new HashSet<Member>());
                    }
                    mapOfSlicerMembers.get(hierarchy).add(slicerMember);
                }
            }

            // Now we have the non-List-Members, but some of them may not be
            // All Members (default Member need not be the All Member) and
            // for some Hierarchies there may not be an All Member.
            // So we create an array of Objects some elements of which are
            // All Members and others elements will be an array of all top-level
            // Members when there is not an All Member.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            allMemberList = new ArrayList<Member>();
            List<Member[]> nonAllMemberList = new ArrayList<Member[]>();

            Member em;
            boolean isSlicerMember;
            for (Member evalMember : evalMembers) {
                em = evalMember;

                isSlicerMember =
                    slicerMembers != null
                        && slicerMembers.contains(em);

                if (em == null) {
                    // Above we might have removed some by setting them
                    // to null. These are the CrossJoin axes.
                    continue;
                }
                if (em.isMeasure()) {
                    continue;
                }

                //
                // The unconstrained members need to be replaced by the "All"
                // member based on its usage and property. This is currently
                // also the behavior of native cross join evaluation. See
                // SqlConstraintUtils.addContextConstraint()
                //
                // on slicer? | calculated? | replace with All?
                // -----------------------------------------------
                //     Y      |      Y      |      Y always
                //     Y      |      N      |      N
                //     N      |      Y      |      N
                //     N      |      N      |      Y if not "All"
                // -----------------------------------------------
                //
                if ((isSlicerMember && !em.isCalculated())
                    || (!isSlicerMember && em.isCalculated()))
                {
                    // If the slicer contains multiple members from this one's
                    // hierarchy, add them to nonAllMemberList
                    if (isSlicerMember) {
                        Set<Member> hierarchySlicerMembers =
                            mapOfSlicerMembers.get(em.getHierarchy());
                        if (hierarchySlicerMembers.size() > 1) {
                            nonAllMemberList.add(
                                hierarchySlicerMembers.toArray(
                                    new Member[hierarchySlicerMembers.size()]));
                        }
                    }
                    continue;
                }

                // If the member is not the All member;
                // or if it is a slicer member,
                // replace with the "all" member.
                if (isSlicerMember || !em.isAll()) {
                    Hierarchy h = em.getHierarchy();
                    final List<Member> rootMemberList =
                        schemaReader.getHierarchyRootMembers(h);
                    if (h.hasAll()) {
                        // The Hierarchy has an All member
                        boolean found = false;
                        for (Member m : rootMemberList) {
                            if (m.isAll()) {
                                allMemberList.add(m);
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            System.out.println(
                                "CrossJoinFunDef.nonEmptyListNEW: ERROR");
                        }
                    } else {
                        // The Hierarchy does NOT have an All member
                        Member[] rootMembers =
                            rootMemberList.toArray(
                                new Member[rootMemberList.size()]);
                        nonAllMemberList.add(rootMembers);
                    }
                }
            }
            nonAllMembers =
                nonAllMemberList.toArray(
                    new Member[nonAllMemberList.size()][]);

            query.putEvalCache(allMemberListKey, allMemberList);
            query.putEvalCache(nonAllMembersKey, nonAllMembers);
        }

        //
        // Determine if there is any data.
        //
        // Put all of the All Members into Evaluator
        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setContext(allMemberList);
            // Iterate over elements of the input list. If for any
            // combination of
            // Measure and non-All Members evaluation is non-null, then
            // add it to the result List.
            final TupleCursor cursor = list.tupleCursor();
            int i=0;
            int checkedI=0;
            if(nextStart>=0){
            checkedI=start;	
            }
            int rec=0;
            while (cursor.forward()) {
            	i++;
            	if(i>nextStart||nextStart<0){
            		if(rec>=count){
            			break;}
                cursor.setContext(evaluator);
                if (checkData(
                        nonAllMembers,
                        nonAllMembers.length - 1,
                        measureSet,
                        evaluator))
                {
                	if(checkedI++>=start){
                    result.addCurrent(cursor);
                    rec++;
                	}
                }
            	}
            }
            evaluator.setAttribute("nextStart", i-1);
            return result;
        } finally {
            evaluator.restore(savepoint);
        }
    }
    /**
     * Return <code>true</code> if for some combination of Members
     * from the nonAllMembers array of Member arrays and Measures from
     * the Set of Measures evaluate to a non-null value. Even if a
     * particular combination is non-null, all combinations are tested
     * just to make sure that the data is loaded.
     *
     * @param nonAllMembers array of Member arrays of top-level Members
     * for Hierarchies that have no All Member.
     * @param cnt which Member array is to be processed.
     * @param measureSet Set of all that should be tested against.
     * @param evaluator the Evaluator.
     * @return True if at least one combination evaluated to non-null.
     */
    private static boolean checkData(
        Member[][] nonAllMembers,
        int cnt,
        Set<Member> measureSet,
        Evaluator evaluator)
    {
        if (cnt < 0) {
            // no measures found, use standard algorithm
            if (measureSet.isEmpty()) {
                Object value = evaluator.evaluateCurrent();
                if (value != null
                    && !(value instanceof Throwable))
                {
                    return true;
                }
            } else {
                // Here we evaluate across all measures just to
                // make sure that the data is all loaded
                boolean found = false;
                for (Member measure : measureSet) {
                    evaluator.setContext(measure);
                    Object value = evaluator.evaluateCurrent();
                    if (value != null
                        && !(value instanceof Throwable))
                    {
                        found = true;
                    }
                }
                return found;
            }
        } else {
            boolean found = false;
            for (Member m : nonAllMembers[cnt]) {
                evaluator.setContext(m);
                if (checkData(nonAllMembers, cnt - 1, measureSet, evaluator)) {
                    found = true;
                }
            }
            return found;
        }
        return false;
    }
    /**
     * Visitor class used to locate a resolved function call within an
     * expression
     */
    private static class ResolvedFunCallFinder
            extends MdxVisitorImpl
    {
        private final ResolvedFunCall call;
        public boolean found;
        private final Set<Member> activeMembers = new HashSet<Member>();

        public ResolvedFunCallFinder(ResolvedFunCall call)
        {
            this.call = call;
            found = false;
        }

        public Object visit(ResolvedFunCall funCall)
        {
            if (funCall == call) {
                found = true;
            }
            return null;
        }

        public Object visit(MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            if (member.isCalculated()) {
                if (activeMembers.add(member)) {
                    Exp memberExp = member.getExpression();
                    memberExp.accept(this);
                    activeMembers.remove(member);
                }
            }
            return null;
        }
    }

    /**
     * Traverses the function call tree of
     * the non empty crossjoin function and populates the queryMeasureSet
     * with base measures
     */
    private static class MeasureVisitor extends MdxVisitorImpl {

        private final Set<Member> queryMeasureSet;
        private final ResolvedFunCallFinder finder;
        private final Set<Member> activeMeasures = new HashSet<Member>();

        /**
         * Creates a MeasureVisitor.
         *
         * @param queryMeasureSet Set of measures in query
         *
         * @param crossJoinCall Measures referencing this call should be
         * excluded from the list of measures found
         */
        MeasureVisitor(
            Set<Member> queryMeasureSet,
            ResolvedFunCall crossJoinCall)
        {
            this.queryMeasureSet = queryMeasureSet;
            this.finder = new ResolvedFunCallFinder(crossJoinCall);
        }

        public Object visit(ParameterExpr parameterExpr) {
            final Parameter parameter = parameterExpr.getParameter();
            final Type type = parameter.getType();
            if (type instanceof mondrian.olap.type.MemberType) {
                final Object value = parameter.getValue();
                if (value instanceof Member) {
                    final Member member = (Member) value;
                    process(member);
                }
            }

            return null;
        }

        public Object visit(MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            process(member);
            return null;
        }

        private void process(final Member member) {
            if (member.isMeasure()) {
                if (member.isCalculated()) {
                    if (activeMeasures.add(member)) {
                        Exp exp = member.getExpression();
                        finder.found = false;
                        exp.accept(finder);
                        if (! finder.found) {
                            exp.accept(this);
                        }
                        activeMeasures.remove(member);
                    }
                } else {
                    queryMeasureSet.add(member);
                }
            }
        }
    }
  
}

// End SubsetFunDef.java
