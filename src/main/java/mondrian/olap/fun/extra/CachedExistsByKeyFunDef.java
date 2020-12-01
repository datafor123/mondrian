/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2019 Hitachi Vantara..  All rights reserved.
*/

package mondrian.olap.fun.extra;

import java.util.HashMap;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ListCalc;
import mondrian.calc.StringCalc;
import mondrian.calc.TupleCalc;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.fun.FunDefBase;

/**
 * CachedExistsByKeyFunDef is a general cache for dynamic calculate-set
 * 
 * @author Gcy
 *
 */
public class CachedExistsByKeyFunDef extends FunDefBase {
  public static final CachedExistsByKeyFunDef instance = new CachedExistsByKeyFunDef();

  CachedExistsByKeyFunDef() {
    super( "CachedExistsByKey",
        "Returns tuples from a non-dynamic <Set> that exists in the specified <Tuple>.  This function will build a query level cache named <String> based on the <Tuple> type.",
        "fxxtS" );
  }

  public Calc compileCall( ResolvedFunCall call, ExpCompiler compiler ) {
    final ListCalc listCalc1 = compiler.compileList( call.getArg( 0 ) );
    final TupleCalc tupleCalc1 = compiler.compileTuple( call.getArg( 1 ) );
    final StringCalc stringCalc = compiler.compileString( call.getArg( 2 ) );

    return new AbstractListCalc( call, new Calc[] { listCalc1, tupleCalc1, stringCalc } ) {
      public TupleList evaluateList( Evaluator evaluator ) {
        Member[] subtotal = tupleCalc1.evaluateTuple( evaluator );
        String namedSetName = stringCalc.evaluateString( evaluator );
        String mainKey = makeSetCacheKey( namedSetName, subtotal );
        String subtotalKey = makeSubtotalKey(subtotal);
        HashMap<String, TupleList> setCache = (HashMap<String, TupleList>) evaluator.getQuery().getEvalCache( mainKey);
        if ( setCache != null ) {
          TupleList tuples = setCache.get(subtotalKey);
          if ( tuples == null ) {
            tuples = TupleCollections.emptyList( listCalc1.getType().getArity() );
          }
          return tuples;
        }
        // Build cache
        setCache = new HashMap<String, TupleList>();
        TupleList setToCache = listCalc1.evaluateList( evaluator );
        if(evaluator instanceof mondrian.rolap.RolapEvaluator){
        	if(!((mondrian.rolap.RolapEvaluator)evaluator).getCellReader().isDirty()){
        	setCache.put( subtotalKey, setToCache );
            evaluator.getQuery().putEvalCache( mainKey, setCache );
        	}
  	    }else{
  	    	setCache.put( subtotalKey, setToCache );
            evaluator.getQuery().putEvalCache( mainKey, setCache );
  	    }
        TupleList tuples = setCache.get(subtotalKey);
        if ( tuples == null ) {
          tuples = TupleCollections.emptyList( listCalc1.getType().getArity() );
        }
        return tuples;
      }
    };
  }
  
  private String makeSetCacheKey( String setName, Member[] members ) {
    StringBuilder builder = new StringBuilder();
    builder.append( setName );
    for ( Member m : members ) {
      builder.append( m.getUniqueName() );
    }
    return builder.toString();
  }

  private String makeSubtotalKey( Member[] members ) {
    StringBuilder builder = new StringBuilder();
    for ( Member m : members ) {
      builder.append( m.getUniqueName() );
    }
    return builder.toString();
  }

}