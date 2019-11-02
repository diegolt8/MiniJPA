package test;

import persistence.Persistence;

/**
 * @author INDRA <br>
 *         Claudia Patricia Fernandez Quitian<br>
 *         cpfernandezq@indracompany.com<br>
 * 
 * @date 23/10/2019
 * @version 1.0
 */
public class Prueba2 {
	
	public static void main(String[] args) {
		
		 Persistence persistence = new Persistence();
		try {
			//Me trae todos los registros
			//ResultSet result = persistence.getAllPersons(new Persona());
			//Busco Por id
			//persistence.deletePerson(new Persona(), "Diego");
			/*while (resultById.next()) {
				System.out.println(resultById.getString("id"));
				
			}*/
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
