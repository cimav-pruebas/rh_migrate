/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package migrarrhoracletopostgres;

import com.google.common.base.Strings;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author juan.calderon
 */
public class MigrarRHOracleToPostgres {

    public static String CIMAV_15_XDB = "jdbc:oracle:thin:@//10.1.0.44:1521/cimavXDB.netmultix.cimav.edu.mx";
    public static String RH_DEVELOPMENT = "jdbc:postgresql://10.0.4.40:5432/rh_development";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here

        int opcion = 1;

        switch (opcion) {
            case 0:
                vaciarEmpleados();
                break;
            case 1:
                migrarEmpleados();
                break;
            case 2:
                break;
            case 3:
                migrarTabulador();
                break;
            case 4:
                migrarDepartamentos();
                break;
            case 5:
                migrarConceptos();
                break;
            default:
                System.out.println("Default");
        }
    }
    
    private static void migrarTabulador() {
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver postgresDriver = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(postgresDriver);

            Connection connOracle = DriverManager.getConnection(CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_DEVELOPMENT, "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPostgres = connPostgres.createStatement()) {
                
                // Vaciar tabulador
                String sql = "DELETE FROM Tabulador;"; 
                stmtPostgres.executeUpdate(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE tabulador_id_seq RESTART WITH 1;";
                stmtPostgres.executeUpdate(sql);
                
                // sacar lista ordenada de Categorias unicas y usadas
                String sqlCategoriasExistentes = "select distinct e.NO01_CATEGORIA, t.* from NO01 e, NO22 t where e.NO01_CATEGORIA = t.NO22_CATEGORIA  order by e.NO01_CATEGORIA";
                ResultSet rsOra = stmtOra.executeQuery(sqlCategoriasExistentes);
                while (rsOra.next()) {
                    String catego = rsOra.getString("NO01_CATEGORIA").trim();
                    String nombre = rsOra.getString("NO22_nombre").trim();
                    String sueldo = rsOra.getString("NO22_sueldo").trim();
                    String materiales = rsOra.getString("NO22_mat_didac");
                    String compensacion = rsOra.getString("NO22_comp_garan").trim();
                    String honorarios = rsOra.getString("NO22_honorarios").trim();
                    String carga = rsOra.getString("NO22_carga_adm").trim();
                    
                    // insertar el registro en Tabulador
                    sql = "INSERT INTO Tabulador VALUES (DEFAULT, '" + catego + "', '" + nombre + "', " + sueldo + ", " 
                           + materiales + ", " + compensacion + ", " + honorarios + ", " + carga +");";
                    stmtPostgres.executeUpdate(sql);
                }
                rsOra.close();
                
            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
                connOracle.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private static void migrarDepartamentos() {
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver postgresDriver = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(postgresDriver);

            Connection connOracle = DriverManager.getConnection(CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_DEVELOPMENT, "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPostgres = connPostgres.createStatement()) {
                
                // Vaciar departamentos
                String sql = "DELETE FROM Departamentos;"; 
                stmtPostgres.executeUpdate(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE departamentos_id_seq RESTART WITH 1;";
                stmtPostgres.executeUpdate(sql);
                
                // sacar lista ordenada de Categorias unicas y usadas
                String sqlCategoriasExistentes = "select distinct e.NO01_DEPTO, d.* from NO01 e, NO20 d where trim(e.NO01_DEPTO) = trim(d.NO20_DEPTO) order by e.NO01_DEPTO";
                try (ResultSet rsOra = stmtOra.executeQuery(sqlCategoriasExistentes)) {
                    while (rsOra.next()) {
                        String depto = rsOra.getString("NO01_Depto").trim();
                        String nombre = rsOra.getString("NO20_nombre").trim();
                        
                        depto = Strings.padStart(depto, 5, '0');
                        
                        // insertar el registro en Departamentos
                        
                        // Si el Depto es vacio, se lanza un Trigger
                        // Si el Depto No es vacio, se inserta directo.
                        sql = "INSERT INTO Departamentos VALUES (default, '" + depto + "', '" + nombre +"', 0 );";
                        
                        stmtPostgres.executeUpdate(sql);
                    }
                }
                
            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
                connOracle.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
 private static void vaciarEmpleados() {
        try {
            Driver postgresDriver = new org.postgresql.Driver();
            DriverManager.registerDriver(postgresDriver);

            Connection connPostgres = DriverManager.getConnection(RH_DEVELOPMENT, "rh_user", "rh_1ser");

            try (Statement stmtPostres = connPostgres.createStatement();) {
                String sql = "DELETE FROM Empleados;";
                stmtPostres.execute(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE empleados_id_seq RESTART WITH 1;";
                stmtPostres. executeUpdate(sql);
                
                
            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    private static void migrarEmpleados() {

        boolean migrarJefes = true;
        
        /* Generar todos registros de Empleados desde N001 */
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connOracle = DriverManager.getConnection(CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_DEVELOPMENT, "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPostgress = connPostgres.createStatement()) {
                
                // VACIAR NominaQuincenal
                String sql = "DELETE FROM NominaQuincenal;";
                stmtPostgress.execute(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE nominaquincenal_id_seq RESTART WITH 1;";
                stmtPostgress.execute(sql);

                // VACIAR Empleados
                sql = "DELETE FROM Empleados;";
                stmtPostgress.execute(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE empleados_id_seq RESTART WITH 1;";
                stmtPostgress.execute(sql);
                
                // Leer empleados de NetMultix; excepto Bajas. NO01_FECHA_SAL es la fecha de baja.
                sql = "SELECT e.* FROM NO01 e where e.NO01_STATUS != 'B'"; // AND e.NO01_CVE_EMP like '%0076%'";
                ResultSet rsOra = stmtOra.executeQuery(sql);

                while (rsOra.next()) {
                    String cveEmp = rsOra.getString("NO01_CVE_EMP").trim();
                    int consecutivo = Integer.parseInt(cveEmp);
                    cveEmp = stringQuoted(rsOra.getString("NO01_CVE_EMP"));
                    String nomEmp = stringQuoted(rsOra.getString("NO01_NOM_EMP"));
                    
                    String cveDepto = rsOra.getString("NO01_depto").trim();
                    cveDepto = Strings.padStart(cveDepto, 5, '0');
                    
                    String rfc = stringQuoted(rsOra.getString("NO01_RFC_EMP"));
                    String imss = stringQuoted(rsOra.getString("NO01_CVE_IMSS"));
                    String curp = stringQuoted(rsOra.getString("NO01_CURP"));

                    String fechaIng = makeDate(rsOra.getString("NO01_FECHA_ING"));

                    String categoria = rsOra.getString("NO01_CATEGORIA").trim();
                    String nomBanco = stringQuoted(rsOra.getString("NO01_NOM_BANCO"));
                    int idBanco = nomBanco.contains("BANORTE") ? 0 : 9;
                    String cuentaBanco = stringQuoted(rsOra.getString("NO01_CTA_BANCO"));
                    String regimen = stringQuoted(rsOra.getString("NO01_REGIMEN"));
                    int idGrupo = 0;
                    if (regimen.contains("AYA")) {
                        idGrupo = 1;
                    } else if (regimen.contains("CYT")) {
                        idGrupo = 2;
                    } else if (regimen.contains("MMS")) {
                        idGrupo = 3;
                    }
                    if (regimen.contains("HON")) {
                        idGrupo = 4;
                    }
                    int idTipoAntiguedad = 0;
                    String tipoAnt = stringQuoted(rsOra.getString("NO01_TIPO_ANT"));
                    if (null != tipoAnt) {
                        switch (tipoAnt) {
                            case "I": // Investigación
                                idTipoAntiguedad = 1;
                                break;
                            case "S": // Sin Antigüedad
                                idTipoAntiguedad = 0;
                                break;
                            case "A": // Administrativa
                                idTipoAntiguedad = 2;
                                break;
                        }
                    }
                    int idSede = 0;
                    String ubicacion = stringQuoted(rsOra.getString("NO01_UBICACION"));
                    if (null != ubicacion) {
                        switch (ubicacion) {
                            case "1": // chi
                                idSede = 0;
                                break;
                            case "2": // Juárez (no hay)
                                idSede = 3;
                                break;
                            case "3": // mty
                                idSede = 1;
                                break;
                            case "4": //dgo
                                idSede = 2;
                                break;
                        }
                    }

                    String fechaIniContrato = makeDate(rsOra.getString("NO01_INI_CONTRATO"));
                    String fechaFinContrato = makeDate(rsOra.getString("NO01_FIN_CONTRATO"));

                    int idTipoContrato = rsOra.getInt("NO01_TIPO_CONTRATO");
                    int idTipoSni = 0;
                    String sni = stringQuoted(rsOra.getString("NO01_SNI"));
                    if ("NO APLICA".equals(sni.trim())) {
                        idTipoSni = 0;
                    } else if ("CANDIDATO".equals(sni.trim())) {
                        idTipoSni = 1;
                    } else if ("NIVEL I".equals(sni.trim())) {
                        idTipoSni = 2;
                    } else if ("NIVEL II".equals(sni.trim())) {
                        idTipoSni = 3;
                    } else if ("NIVEL III".equals(sni.trim())) {
                        idTipoSni = 4;
                    }
                    String numSni = stringQuoted(rsOra.getString("NO01_NUM_SNI"));

                    String fechaSNI = makeDate(rsOra.getString("NO01_FECHA_SNI"));

                    //String numCredito = rsOra.getString("NO01_ID_CREDITO").trim();  // Descartarlo.
                    String apellidoPAt = rsOra.getString("NO01_APELLIDO_PAT").trim();
                    String apellidoMat = rsOra.getString("NO01_APELLIDO_MAT").trim();
                    String nombre = rsOra.getString("NO01_NOMBRE").trim();
                    String proyecto = rsOra.getString("NO01_PROYECTO").trim(); // TODO: Falta proyectos
                    String clinica = stringQuoted(rsOra.getString("NO01_CLINICA"));
                    int idClinica = EClinica.getId(clinica);

//                    String jefe = rsOra.getString("NO01_JEFE");
//                    int idJefe = 0;
//                    String codeJefe = "";
//                    String nameJefe = "";
//                    sql = "select * from empleados e where e.code = '" + jefe.trim() + "';";
//                    ResultSet rsPost = stmtPost.executeQuery(sql);
//                    while (rsPost.next()) {
//                        idDepto = rsPost.getInt("id");
//                        codigoDepto = rsPost.getString("codigo");
//                        nomDepto = rsPost.getString("nombre");
//                    }                    
                    int idDepto = 0;
                    String codigoDepto = "";
                    String nomDepto = "";
                    sql = "select * from departamentos d where d.code like '%" + cveDepto.trim() + "';";
                    cveDepto = stringQuoted(cveDepto);
                    ResultSet rsPostgress = stmtPostgress.executeQuery(sql);
                    while (rsPostgress.next()) {
                        idDepto = rsPostgress.getInt("id");
                        codigoDepto = rsPostgress.getString("code");
                        nomDepto = rsPostgress.getString("name");
                    }
                    rsPostgress.close();
                    int idTabulador = 0;
                    String codeTabulador = "";
                    String nameTabulador = "";
                    sql = "select * from tabulador d where d.code = '" + categoria.trim() + "';";
                    categoria = stringQuoted(categoria);
                    rsPostgress = stmtPostgress.executeQuery(sql);
                    while (rsPostgress.next()) {
                        idTabulador = rsPostgress.getInt("id");
                        codeTabulador = rsPostgress.getString("code");
                        nameTabulador = rsPostgress.getString("name");
                    }
                    rsPostgress.close();
                    
                    String cuentaCimav = rsOra.getString("NO01_EMAIL60");
                    cuentaCimav = cuentaCimav != null && !cuentaCimav.trim().isEmpty() ? cuentaCimav.split("@")[0] : "default";

                    // Fecha de antiguedad es la de CALCULO; si no tiene, usar la de INGRESO
                    String fechaAntiguedad = makeDate(rsOra.getString("NO01_FECHA_CAL"));
                    fechaAntiguedad = fechaAntiguedad == null || fechaAntiguedad.trim().isEmpty() ? fechaIng : fechaAntiguedad;

                    int idStatus = 0;
                    int idProyecto = 0;
                    
                    String urlPhoto = stringQuoted("http://www.cimav.edu.mx/foto/" + cuentaCimav);
                    cuentaCimav = stringQuoted(cuentaCimav);
                    
                    int idTipoEmpleado = 0;
                    
                    // Fecha de Baja es la FECHA_SAL que normalmente debe coincidir con FEC_FINIQUITO
                    // y debe tener el STATUS = 'B'
                    String fechaBaja =  makeDate(rsOra.getString("NO01_FECHA_SAL")); // ninguno debe tener Fecha_SAL (Baja)
                    if (fechaBaja != null) {
                        System.out.println("FECHA BAJA: " + cveEmp + " >> " + fechaBaja);
                    }
                            
                    String dirCalle = stringQuoted(rsOra.getString("NO01_DIRECCION"));
                    String dirColonia = stringQuoted(rsOra.getString("NO01_COLONIA"));
                    String dirCP = stringQuoted(rsOra.getString("NO01_CP"));
                    String telefono = stringQuoted(rsOra.getString("NO01_TELEFONO1") + ",  " + rsOra.getString("NO01_TELEFONO2"));
                    String emailPersonal = stringQuoted(rsOra.getString("NO01_EMAIL60"));// stringQuoted(rsOra.getString("NO01_CORREO1") + "; " + rsOra.getString("NO01_CORREO2"));
                    
                    String fechaNacimiento = makeDate(rsOra.getString("NO01_FECHA_NAC"));
                    
                    String idSexo = rsOra.getString("NO01_SEXO");
                    idSexo = idSexo != null && idSexo.contains("M") ? "0" : "1";
                    
                    String idEdoCivil = rsOra.getString("NO01_EDO_CIVIL");
                    if (idEdoCivil != null) {
                        idEdoCivil = idEdoCivil.trim().toUpperCase();
                        if (idEdoCivil.contains("SOLT")) {
                            idEdoCivil = "0";
                        } else if (idEdoCivil.contains("CASAD")) {
                            idEdoCivil = "1";
                        } else if (idEdoCivil.contains("DIVOR")) {
                            idEdoCivil = "2";
                        } else if (idEdoCivil.contains("VIUDO")) {
                            idEdoCivil = "3";
                        } else if (idEdoCivil.contains("UNION")) {
                            idEdoCivil = "4";
                        } else {
                            idEdoCivil = null;
                        } 
                    } 
                    
                    String name = stringQuoted(apellidoPAt + " " + apellidoMat + " " + nombre);
                    apellidoPAt = stringQuoted(apellidoPAt);
                    apellidoMat = stringQuoted(apellidoMat);
                    nombre = stringQuoted(nombre);

//                    cveEmp = "'" + cveEmp + "'";
//                    curp = "'" + curp + "'";
//                    rfc = "'" + rfc + "'";
//                    imss = "'" + imss + "'";
//                    cuentaBanco = "'" + cuentaBanco + "'";
//                    urlPhoto = "'" + urlPhoto + "'";
//                    name = "'" + name + "'";
//                    apellidoPAt = "'" + apellidoPAt + "'";
//                    apellidoMat = "'" + apellidoMat + "'";
//                    nombre = "'" + nombre + "'";
//                    //numCredito = "'" + numCredito + "'";
//                    cuentaCimav = "'" + cuentaCimav + "'";
//                    numSni = "'" + numSni + "'";

                    String sqlMigrarEmpleado = "INSERT INTO empleados VALUES ( default, "
                            + cveEmp + ", " + consecutivo + ", " + idDepto + ", " + idStatus + ", " + curp + ", " + rfc + ", " + imss + ", " + idProyecto + ", " + cuentaBanco
                            + ", " + urlPhoto + ", " + name + ", " + apellidoPAt + ", " + apellidoMat + ", " + idGrupo + ", " + nombre + ", " + idTabulador + ", " + idClinica
                            + ", " + cuentaCimav + ", " + idBanco + ", " + idSede + ", " + idTipoEmpleado + ", " + idTipoContrato
                            + ", " + fechaIng + ", " + fechaIniContrato + ", " + fechaFinContrato + ", " + fechaBaja + ", " + idTipoAntiguedad + ", " + fechaAntiguedad
                            + ", " + idTipoSni + ", " + numSni + ", " + fechaSNI 
                            + ", NULL, " + fechaNacimiento + ", " + idSexo + ", " + idEdoCivil + ", " + dirCalle + ", " + dirColonia + ", " + dirCP + ", " + telefono + ", " + emailPersonal
                            + " );";

                    //System.out.println("" + sqlMigration);
                    
//                            + "NULL, 19671221, F, CASADO (A)     , 'C. 33 3411                              ', 'BARRIO DE LONDRES             ', '31060', '              ,                ', '08019                         ;                               ' );"
                    
                    stmtPostgress.execute(sqlMigrarEmpleado);
                }
                
            } catch (Exception e2) {
                migrarJefes = false;
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
                connOracle.close();
            }
            
            if (migrarJefes) {
              migrarJefes();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static String stringQuoted(String str) {
        if (str == null || str.trim().isEmpty()) {
            return null;
        } else {
            return "'"  + str.trim() + "'";
         }
    }

    private static void migrarJefes() {

        /* Genera los update para Insertar los Jefes en Empleados.
            Para migrar jefes, Empleados ya debe tener a todos los Empleados */
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connOracle = DriverManager.getConnection(CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_DEVELOPMENT, "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPostgress = connPostgres.createStatement()) {
                String sql = "SELECT e.NO01_CVE_EMP, e.NO01_JEFE FROM NO01 e where e.NO01_STATUS != 'B'";
                ResultSet rsOra = stmtOra.executeQuery(sql);

                while (rsOra.next()) {
                    String cveEmp = rsOra.getString("NO01_CVE_EMP").trim();
                    String jefe = rsOra.getString("NO01_JEFE").trim();

                    int idJefe = 0;
                    sql = "select e.id from empleados e where e.code = '" + jefe.trim() + "';";
                    ResultSet rsPost = stmtPostgress.executeQuery(sql);
                    while (rsPost.next()) {
                        idJefe = rsPost.getInt("id");
                    }
                    String sqlUpdateJefe = "UPDATE empleados SET id_jefe = " + idJefe + " WHERE code = '" + cveEmp + "';";

                    //System.out.println("" + sqlMigration);
                    stmtPostgress.execute(sqlUpdateJefe);
                }

            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connOracle.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static String makeDate(String fechaOra) {
        java.sql.Date fechaSql = null;
        if (fechaOra != null && !fechaOra.trim().isEmpty() && fechaOra.trim().length() == 8) {
            try {
                Date fecha = new SimpleDateFormat("yyyyMMdd").parse(fechaOra);
                fechaSql = new java.sql.Date(fecha.getTime());
            } catch (ParseException ex) {
                Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        String result = fechaSql == null ? null : "'" + fechaSql.toString() + "'";
        return result;
    }

    private static void migrarConceptos() {
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver postgresDriver = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(postgresDriver);

            Connection connOracle = DriverManager.getConnection(CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_DEVELOPMENT, "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPostgres = connPostgres.createStatement()) {
                
                // Vaciar departamentos
                String sql = "DELETE FROM Conceptos;"; 
                stmtPostgres.executeUpdate(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE conceptos_id_seq RESTART WITH 1;";
                stmtPostgres.executeUpdate(sql);
                
                // sacar lista ordenada de Conceptos (Percepciones y Deducciones) capturables
                String sqlConceptos = "select c.* from no04 c where c.no04_tmovto in ('P','D') order by c.no04_conce";
                try (ResultSet rsOra = stmtOra.executeQuery(sqlConceptos)) {
                    while (rsOra.next()) {
                        String code = rsOra.getString("NO04_CONCE").trim();
                        String nombre = rsOra.getString("NO04_nombre");
                        String tipoMvto = rsOra.getString("NO04_tmovto").trim();
                        String tipoCalculo = "F"; // Fijo por Default. No hay ninguna columna que me lo indique
                        
                        code = Strings.padStart(code, 5, '0');
                        code = stringQuoted(code);
                        nombre = stringQuoted(nombre);
                        tipoMvto = stringQuoted(tipoMvto);
                        tipoCalculo = stringQuoted(tipoCalculo);                        
                        
                        // insertar el registro en Conceptos
                        
                        // Si el Depto es vacio, se lanza un Trigger
                        // Si el Depto No es vacio, se inserta directo.
                        sql = "INSERT INTO Conceptos VALUES (default, " + code + ", " + nombre + ", " + tipoMvto + ", 0, " + tipoCalculo + ");";
                        
                        stmtPostgres.executeUpdate(sql);
                    }
                }
                
            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
                connOracle.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
