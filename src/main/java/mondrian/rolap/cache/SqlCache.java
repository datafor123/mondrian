package mondrian.rolap.cache;

import mondrian.olap.Util.PropertyList;
import mondrian.rolap.RolapConnectionProperties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * gcy effect to cache sql result
 * @author Gcy
 *
 * */
public class SqlCache {
	public final static String  CONNECTION_KEY="UNIQUE_CONNECTION";
	public final static String  DIALECT_KEY="UNIQUE_STATEMENT";
	/** The singleton. */
	private static SqlCache instance = new SqlCache();
	private  Map<String,Integer> CACHE;
	private  Map<String,Object> COMPLEX_CACHE;
	private Set<String> CONNECTION_NAMES;
	/** Returns the singleton. */
	public static SqlCache instance() {
		return instance;
	}
	private SqlCache() {
		CACHE=new HashMap<String,Integer>();
		COMPLEX_CACHE=new HashMap<String,Object>();
		CONNECTION_NAMES=new HashSet<String>();
	}

	public String createUniqueConnectionKey(PropertyList connectInfo){
		String jdbcUser =
				connectInfo.get(RolapConnectionProperties.JdbcUser.name());
		String jdbcConnectString =
				connectInfo.get(RolapConnectionProperties.Jdbc.name());

		String result=jdbcConnectString+";user="+jdbcUser+";"+ SqlCache.CONNECTION_KEY;
		return result;
	}
	public String createUniqueDialectKey(PropertyList connectInfo){
		String jdbcUser =
				connectInfo.get(RolapConnectionProperties.JdbcUser.name());
		String jdbcConnectString =
				connectInfo.get(RolapConnectionProperties.Jdbc.name());
		String catalogUrl =
				connectInfo.get(RolapConnectionProperties.Catalog.name());
		String result=jdbcConnectString+";user="+jdbcUser+";catalog="+catalogUrl+";"+ SqlCache.DIALECT_KEY;
		return result;
	}
	public String createUniqueMdxKey(PropertyList connectInfo, String mdx){
		String jdbcUser =
				connectInfo.get(RolapConnectionProperties.JdbcUser.name());
		String jdbcConnectString =
				connectInfo.get(RolapConnectionProperties.Jdbc.name());
		String catalogUrl =
				connectInfo.get(RolapConnectionProperties.Catalog.name());
		String result=jdbcConnectString+";user="+jdbcUser+";catalog="+catalogUrl+";"+mdx;
		return result;
	}
	public boolean useCache(PropertyList connectInfo){
		return "T".equals(connectInfo.get("CacheStatement"));
	}
	public void setValue(String key,int value){
		CACHE.put(key, value);
	}
	public Integer getValue(String key){
		return CACHE.get(key);
	}
	public void setObject(String key,Object value){
		COMPLEX_CACHE.put(key, value);
	}
	public Object getObject(String key){
		return COMPLEX_CACHE.get(key);
	}
	public boolean isConnectionContains(String name){
		return CONNECTION_NAMES.contains(name);
	}
	public void addConnection(String name){
		CONNECTION_NAMES.add(name);
	}
}
