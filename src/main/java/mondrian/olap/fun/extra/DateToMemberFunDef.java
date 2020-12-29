/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun.extra;


import java.text.ParseException;
import java.util.List;
import mondrian.calc.Calc;
import mondrian.calc.DateTimeCalc;
import mondrian.calc.DoubleCalc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.LevelCalc;
import mondrian.calc.StringCalc;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractMemberCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Annotation;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.Validator;
import mondrian.olap.fun.FunDefBase;
import mondrian.olap.fun.ReflectiveMultiResolver;
import mondrian.olap.fun.Resolver;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.Type;

/**
 * Definition of the <code>DateToMember</code>  MDX extension
 * functions.
 *
 * <p>These functions are not standard MDX.
 *
 * @author Gechenying
 * @mail shareluck_gcy@163.com
 * @since  2019-07-19
 */
public class DateToMemberFunDef extends FunDefBase {
	/*public static final FunDef INSTANCE = new DateToMemberFunDef();

    private DateToMemberFunDef() {
        super(
        		 "DateToMember",
                 "Returns Member on designated Timelevel from DateTime,flag in(<|<=|=|>|>=)",
                "fmlSD");
    }*/
	public static final Resolver Resolver =
	        new ReflectiveMultiResolver(
	            "DateToMember",
	            "DateToMember(<Level>,<Compare>,<DateTime>[,<TimeZone>][,<Locale>])",
	            "Dynamically totals child members specified in a set using a pattern for the total label in the result set.",
	            new String[] {"fmlSD", "fmlSn","fmlSDS", "fmlSnS","fmlSDSS", "fmlSnSS"},
	            DateToMemberFunDef.class);

	    public DateToMemberFunDef(FunDef dummyFunDef) {
	        super(dummyFunDef);
	    }
    public Type getResultType(Validator validator, Exp[] args) {
        return super.getResultType(validator, args);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final LevelCalc levelCalc = compiler.compileLevel(call.getArg(0));
        final StringCalc stringCalc = compiler.compileString(call.getArg(1));
        final DateTimeCalc dateTimeCalc;
        final DoubleCalc numericCalc;
        if(call.getArg(2).getType() instanceof NumericType){
        	numericCalc = compiler.compileDouble(call.getArg(2));
        	dateTimeCalc = null;
        }else{
        	numericCalc = null;
            dateTimeCalc = compiler.compileDateTime(call.getArg(2));
        }
        final StringCalc timeZoneCalc;
        if(call.getArgCount()>3){
        	timeZoneCalc = compiler.compileString(call.getArg(3));
        }else{
        	timeZoneCalc=null;
        }
        final StringCalc localeCalc;
        if(call.getArgCount()>4){
        	localeCalc = compiler.compileString(call.getArg(4));
        }else{
        	localeCalc=null;
        }
        
            return new AbstractMemberCalc(
                    call,
                    new Calc[] {levelCalc, stringCalc})
                {
                    public Member evaluateMember(Evaluator evaluator) {
                        Level level = levelCalc.evaluateLevel(evaluator);
                        String compare = stringCalc.evaluateString(evaluator);
                        java.util.Date date;
                        if(dateTimeCalc==null){
                        	long ts =(long)numericCalc.evaluateDouble(evaluator);
                        	date=new java.util.Date(ts);
                        }else{
                        	date=dateTimeCalc.evaluateDateTime(evaluator);
                        }
                        java.util.TimeZone timeZone=null;
                        if(timeZoneCalc!=null){
                        	String timeZoneName=timeZoneCalc.evaluateString(evaluator);
                        	timeZone=java.util.TimeZone.getTimeZone(timeZoneName);
                        }else{
                        	timeZone=java.util.TimeZone.getTimeZone("GMT+00:00");
                        }
                        java.util.Locale locale=null;
                        if(localeCalc!=null){
                        	String localeName=localeCalc.evaluateString(evaluator);
                        	locale=new java.util.Locale(localeName);
                        }else{
                        	Annotation levelDateLocaleAnnotation=level.getAnnotationMap().get("AnalyzerDateLocale");
                        	if(levelDateLocaleAnnotation!=null){
                        	Object levelDateLocaleValue=levelDateLocaleAnnotation.getValue();
                        	if(levelDateLocaleValue!=null){
                        		String localeName=levelDateLocaleValue.toString();
                        		locale=new java.util.Locale(localeName);
                        	}
                        	}
                        }
                        org.olap4j.metadata.Level.Type type=level.getLevelType();
                        /*if(!type.isTime()){
                        	System.err.println(level.getUniqueName()+" is not Time");
                        	throw new RuntimeException("ERROR:"+level.getUniqueName()+" level type must be Time");
                        	//return level.getHierarchy().getNullMember();
                        }*/
                        String levelDateFormat="";
                        String levelDateFormatStandard="yyyyMMdd";//"yyyyMMddHHmmssSSS"
                        mondrian.rolap.RolapAttribute attr=((mondrian.rolap.RolapLevel)level).getAttribute();
                        mondrian.olap.Property.Datatype datatype=attr.getType();
                        if(mondrian.olap.Property.Datatype.TYPE_DATE.equals(datatype)){
                        	levelDateFormat="[yyyy-MM-dd]";
                        }else if(mondrian.olap.Property.Datatype.TYPE_TIME.equals(datatype)){
                        	levelDateFormatStandard="HHmmss";
                        	levelDateFormat="[HH:mm:ss]";
                        }else if(mondrian.olap.Property.Datatype.TYPE_TIMESTAMP.equals(datatype)){
                        	levelDateFormat="[yyyy-MM-dd HH:mm:ss.S]";
                        	levelDateFormatStandard="yyyyMMddHHmmssS";
                        }
						levelDateFormat=getDateFormatFromParent(level);
                        if(type.equals(org.olap4j.metadata.Level.Type.TIME_YEARS)){
                        	levelDateFormatStandard="yyyy";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_HALF_YEAR)){
                        	levelDateFormatStandard="yyyy";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_QUARTERS)){
                        	levelDateFormatStandard="yyyyMM";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_MONTHS)){
                        	levelDateFormatStandard="yyyyMM";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_WEEKS)){
                        	levelDateFormatStandard="yyyyww";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_DAYS)){
                        	levelDateFormatStandard="yyyyMMdd";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_HOURS)){
                        	levelDateFormatStandard="yyyyMMddHH";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_MINUTES)){
                        	levelDateFormatStandard="yyyyMMddHHmm";
                        }else if(type.equals(org.olap4j.metadata.Level.Type.TIME_SECONDS)){
                        	levelDateFormatStandard="yyyyMMddHHmmss";
                        }
                        java.text.SimpleDateFormat levelFormatStandard=new java.text.SimpleDateFormat(levelDateFormatStandard);
                        levelFormatStandard.setTimeZone(timeZone);
                        String inputDateStrStandard=levelFormatStandard.format(date);
                        long inputDateStrStandardNum=Long.parseLong(inputDateStrStandard);
                        if(type.equals(org.olap4j.metadata.Level.Type.TIME_QUARTERS)){
                        	long m=inputDateStrStandardNum%100;
                        	long m2=(m/4+1)*3;
                        	inputDateStrStandardNum=inputDateStrStandardNum-m+m2;
                        }
                        Member member =null;
                        //System.out.println("gcy trace "+this.getClass().getName()+">>"+new Throwable().getStackTrace()[0].getLineNumber()+">>member:"+member+",level:"+level+",compare:"+compare+",date:"+date+",list:"+list.get(0));//gcy TEST
                        TupleList levelMembers=levelMembers(level, evaluator, false);
                        int hierUniqueNameIndex=level.getHierarchy().getUniqueName().length()+1;
                        int size=levelMembers.size();
                        String uniqueName="";
                        String mDateStringStandard="";
                        java.util.Date mDate;
                        levelDateFormat=levelDateFormat.replaceAll("''", " ");
                        String levelDateFormatAnalyzed=levelDateFormat.replaceAll("q", " MM ");
                        java.text.SimpleDateFormat levelFormat;
                        if(locale==null){
                        	levelFormat=new java.text.SimpleDateFormat(levelDateFormatAnalyzed);
                        }else{
                        	levelFormat=new java.text.SimpleDateFormat(levelDateFormatAnalyzed,locale);
                        }
                        levelFormat.setTimeZone(timeZone);
                        boolean isOrder=false;
                        if(compare.contains(">")){
                    		isOrder=true;
                    	}else{
                    		isOrder=false;
                    	}
                        loop:for(int i=0;i<size;i++){
                        	List<Member> members;
                        	if(isOrder){
                        		members=levelMembers.get(i);
                        	}else{
                        		members=levelMembers.get(size-i-1);
                        	}
                        	int _size=members.size();
                        	for(int j=0;j<_size;j++){
                        		Member m=members.get(j);
                        		uniqueName= m.getUniqueName().substring(hierUniqueNameIndex);
                        		try {
                        		    uniqueName=replaceQuarterValue(uniqueName,levelDateFormat);
									mDate=levelFormat.parse(uniqueName);
									mDateStringStandard=levelFormatStandard.format(mDate);
									switch(compare){
									case ">=":
										if(Long.parseLong(mDateStringStandard)>=inputDateStrStandardNum){
											member=m;
											break loop;
									    }break;
									case "<=":if(Long.parseLong(mDateStringStandard)<=inputDateStrStandardNum){
										member=m;
										break loop;
								    }break;
									case ">":if(Long.parseLong(mDateStringStandard)>inputDateStrStandardNum){
										member=m;
										break loop;
								    }break;
									case "<":if(Long.parseLong(mDateStringStandard)<inputDateStrStandardNum){
										member=m;
										break loop;
								    }break;
									case "=":if(Long.parseLong(mDateStringStandard)==inputDateStrStandardNum){
										member=m;
										break loop;
								    }break;
									default:break;
								    }
									
								} catch (ParseException e) {
									System.err.println("ERROR:"+m.getUniqueName().substring(hierUniqueNameIndex)+" not match format:"+levelDateFormat);
									System.err.println("ERROR:"+uniqueName+" not match format:"+levelDateFormatAnalyzed);
									throw new RuntimeException("ERROR:"+m.getUniqueName().substring(hierUniqueNameIndex)+" not match format:"+levelDateFormat);
									//return level.getHierarchy().getNullMember();
								}catch(RuntimeException e){
									System.err.println("ERROR:"+m.getUniqueName()+", format:"+levelDateFormat+",msg:"+e.getMessage());
									throw new RuntimeException("ERROR:"+m.getUniqueName()+", format:"+levelDateFormat+",msg:"+e.getMessage());
									//return level.getHierarchy().getNullMember();
								}
                        	}
                        }
                        if(member==null){
                        	return level.getHierarchy().getNullMember();
                        }
                        return member;
                    }
                };
    }
    /**Quarter value turn to 2 character month
     * only condition:there is no unfixed length format before q,like m,MMM,d etc.
     * */
    private String replaceQuarterValue(String dateString,String format){
    	String result=dateString;
    	//character position and length consistent
    	format=format.replaceAll("''", " ");
    	format=format.replaceAll("'", "");
    	int length=format.length();
    	for(int i=0;i<length;i++){
    		String a=format.substring(i, i+1);
    		if("q".equals(a)){
    			int qValue=Integer.parseInt(dateString.substring(i,i+1));
    			if(qValue>4||qValue<1){
    				throw new RuntimeException("invalid Quarter value(must be 1,2,3,4)");
    			}
    			qValue=3*qValue;
    			result=result.substring(0,i)+" "+(qValue<9?("0"+qValue):qValue)+" "+result.substring(i+1);
    		}
    	}
    	return result;
    }
	private String getDateFormat(Level level){
		Annotation levelDateAnnotation=level.getAnnotationMap().get("AnalyzerDateFormat");
		if(levelDateAnnotation==null){
			System.err.println("ERROR:"+level.getUniqueName()+"'s AnalyzerDateFormat Undefined");
			throw new RuntimeException("ERROR:"+level.getUniqueName()+"'s AnalyzerDateFormat Undefined");
			//return level.getHierarchy().getNullMember();
		}
		Object levelDateValue=levelDateAnnotation.getValue();
		if(levelDateValue==null){
			System.err.println("ERROR:"+level.getUniqueName()+"'s AnalyzerDateFormat Undefined");
			throw new RuntimeException("ERROR:"+level.getUniqueName()+"'s AnalyzerDateFormat Undefined");
			//return level.getHierarchy().getNullMember();
		}

		return levelDateValue.toString();
	}
	private String getDateFormatFromParent(Level level){
		String levelName=getDateFormat(level);
		if(levelName.contains("].[")){//兼容填写完整版本
			return levelName;
		}else{//从父级追加
			while(level.getParentLevel()!=null&&!level.getParentLevel().isAll()){
				level=level.getParentLevel();
				levelName=getDateFormat(level)+"."+levelName;
			}
			return levelName;
		}
	}
}

// End DateToMemberFunDef.java
