package persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream.GetField;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import annotations.Columna;
import annotations.Entidad;

public class Persistence {

	private static final String PROPERTIES_PATH = "/persistence.properties";

	public void persist(Object obj) throws Exception {

		String query = getSQLPersistQuery(obj);

		System.out.println(query);

		PreparedStatement pstmt = createPreparedStatement(query, obj);
		pstmt.executeUpdate();
	}

	/**
	 * Metodo para que me imprima todo la persona
	 * 
	 * @param obj persona
	 * @return retorna todas las personas
	 * @throws Exception
	 */
	public ResultSet selectPerson(Object obj) throws Exception {
		String query = getAllPersons(obj, null);

		System.out.println(query);

		Statement st = getConnection().createStatement();

		return st.executeQuery(query);
	}

	/**
	 * Metodo que me selecciona la persona por id
	 * 
	 * @param obj objeto persona
	 * @param id  el id que deseo buscar de la persona
	 * @return la ejecucion de la query
	 * @throws Exception
	 */
	public ResultSet selectPersonById(Object obj, String id) throws Exception {
		String query = getPersonsById(obj, id);

		System.out.println(query);

		Statement st = getConnection().createStatement();

		return st.executeQuery(query);
	}

	/**
	 * Metodo para eliminar la persona 
	 * @param obj objeto persona
	 * @param id de la persona que se desea eliminar
	 * @return la ejecucion de la query
	 * @throws SQLException
	 * @throws Exception
	 */
	public boolean deletePerson(Object obj, String id) throws SQLException, Exception {
		String query = deletePersonQuery(obj, id);

		System.out.println(query);

		Statement st = getConnection().createStatement();

		return st.execute(query);
	}
	
	/**
	 * Actualiza la informacion de la persona
	 * @param obj objeto persona
	 * @return la ejecucion de la query
	 * @throws Exception
	 */
	public boolean update(Object obj) throws Exception {
		String query = Persistence.updatePerson(obj);
		Persistence ps = new Persistence();
		PreparedStatement pst = ps.createPreparedStatement(query, obj);
		return pst.execute();
	}

	private Connection getConnection() throws Exception {

		Properties prop = loadProperties();

		Class.forName(prop.getProperty("driver"));
		return DriverManager.getConnection(prop.getProperty("url"), prop.getProperty("user"),
				prop.getProperty("password"));
	}

	private Properties loadProperties() throws IOException {
		Properties prop = new Properties();

		InputStream input = getClass().getResourceAsStream(PROPERTIES_PATH);
		prop.load(input);

		return prop;
	}

	private PreparedStatement createPreparedStatement(String query, Object obj) throws Exception {
		Class clase = obj.getClass();
		List values = getValues(obj);
		System.out.println(values);

		Connection conn = getConnection();

		PreparedStatement pstmt = conn.prepareStatement(query);
		int i = 1;
		for (Object value : values) {
			pstmt.setObject(i, value);
			i++;
		}

		return pstmt;
	}

	private List<Object> getValues(Object obj) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		Class clase = obj.getClass();

		List list = new ArrayList<>();
		Field[] attributes = clase.getDeclaredFields();

		for (Field field : attributes) {

			Columna col = (Columna) field.getAnnotation(Columna.class);

			if (col == null)
				break;

			String getter = "get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
			Method method = clase.getMethod(getter);

			Object value = method.invoke(obj);
			System.out.println(getter + "=" + value);

			list.add(value);
		}

		return list;
	}

	private String getSQLPersistQuery(Object obj) {
		Class clase = obj.getClass();
		StringBuilder query = new StringBuilder();

		Entidad entidad = (Entidad) clase.getAnnotation(Entidad.class);

		if (entidad != null) {
			query.append("INSERT INTO ").append(entidad.schema()).append(".").append(entidad.value());
			System.out.println("Entidad=" + entidad.value() + ",esquema=" + entidad.schema());
		} else {
			query.append("INSERT INTO ").append(clase.getSimpleName());
		}

		query.append("(");

		StringBuilder params = new StringBuilder();
		StringBuilder cols = new StringBuilder();

		Field[] attributes = clase.getDeclaredFields();
		for (Field field : attributes) {
			Columna col = (Columna) field.getAnnotation(Columna.class);

			if (col == null)
				break;

			System.out.println("col_name=" + col.name() + ",is_pk=" + col.isPk());
			cols.append(col.name()).append(",");
			params.append("?").append(",");
		}
		query.append(cols.substring(0, cols.length() - 1)).append(")");
		query.append(" VALUES (").append(params.substring(0, params.length() - 1)).append(")");

		return query.toString();
	}

	private String getAllPersons(Object obj, Object pkId) {
		Class clase = obj.getClass();
		StringBuilder query = new StringBuilder();
		Entidad entidad = (Entidad) clase.getAnnotation(Entidad.class);

		if (entidad != null) {
			query.append("SELECT * FROM ").append(entidad.schema()).append(".").append(entidad.value());
		} else {
			query.append("SELECT * FROM ").append(clase.getSimpleName());
		}

		if (pkId != null) {
			query.append(" WHERE ");

			String str = "";

			Field[] attributes = clase.getDeclaredFields();
			for (Field field : attributes) {
				Columna col = (Columna) field.getAnnotation(Columna.class);

				if (col == null) {
					break;
				}

				if (col.isPk()) {
					str += col.name() + "=";

					switch (pkId.getClass().getSimpleName()) {
					case "String":
						str += "\'" + pkId + "\'";
						break;

					default:
						str += pkId;
					}
				}
			}

			return query + str + ";";
		} else {
			return query.toString();
		}
	}

	private String getPersonsById(Object obj, String pkId) {
		Class clase = obj.getClass();
		StringBuilder query = new StringBuilder();
		Entidad entidad = (Entidad) clase.getAnnotation(Entidad.class);

		if (entidad != null) {
			query.append("SELECT * FROM ").append(entidad.schema()).append(".").append(entidad.value());
		} else {
			query.append("SELECT * FROM ").append(clase.getSimpleName());
		}

		if (pkId != null) {
			query.append(" WHERE ");

			String str = "";

			Field[] attributes = clase.getDeclaredFields();
			for (Field field : attributes) {
				Columna col = (Columna) field.getAnnotation(Columna.class);

				if (col == null) {
					break;
				}

				if (col.isPk()) {
					str += col.name() + "=";

					switch (pkId.getClass().getSimpleName()) {
					case "String":
						str += "\'" + pkId + "\'";
						break;

					default:
						str += pkId;
					}
				}
			}

			return query + str + ";";
		} else {
			return query.toString();
		}
	}

	private String deletePersonQuery(Object obj, String pkId) {
		Class clase = obj.getClass();
		StringBuilder query = new StringBuilder();
		Entidad entidad = (Entidad) clase.getAnnotation(Entidad.class);

		if (entidad != null) {
			query.append("DELETE FROM ").append(entidad.schema()).append(".").append(entidad.value());
		} else {
			query.append("DELETE FROM ").append(clase.getSimpleName());
		}

		if (pkId != null) {
			query.append(" WHERE ");

			String str = "";

			Field[] attributes = clase.getDeclaredFields();
			for (Field field : attributes) {
				Columna col = (Columna) field.getAnnotation(Columna.class);

				if (col == null) {
					break;
				}

				if (col.isPk()) {
					str += col.name() + "=";

					switch (pkId.getClass().getSimpleName()) {
					case "String":
						str += "\'" + pkId + "\'";
						break;
					default:
						str += pkId;
					}
				}
			}

			return query + str + ";";
		} else {
			return query + ";";
		}
	}

	private static String updatePerson(Object obj) {
		Class clase = obj.getClass();
		String query = "";
		Entidad entidad = (Entidad) clase.getAnnotation(Entidad.class);

		if (entidad != null) {
			query += "UPDATE " + entidad.schema() + "." + entidad.value();
		} else {
			query += "UPDATE " + clase.getSimpleName();
		}

		query += " SET ";

		String str = "";

		Field[] attributes = clase.getDeclaredFields();
		Field id = null;
		for (Field field : attributes) {
			Columna col = (Columna) field.getAnnotation(Columna.class);

			if (col == null) {
				break;
			}

			if (!col.isPk()) {
				str += col.name() + "=?,";
			} else {
				id = field;
			}
		}

		query += str.substring(0, str.length() - 1) + " WHERE " + ((Columna) id.getAnnotation(Columna.class)).name()
				+ "=";

		return query + "?;";
	}
}
