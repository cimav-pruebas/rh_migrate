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
    //public static String RH_DEVELOPMENT = "jdbc:postgresql://10.0.4.40:5432/rh_development";
    public static String RH_DEVELOPMENT = "jdbc:postgresql://localhost:5432/rh_development";

    /*
    
    UPDATE empleadoquincenal 
SET sdi_variable_bimestre_anterior = 999.91
FROM empleados 
WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = '00398';
    */
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here

        int opcion = -1;

        switch (opcion) {
            case 0:
                vaciarEmpleados();
                break;
            case 1:
                migrarEmpleados();
                break;
            case 2:
                migrarJefes();
                break;
            case 3:
                migrarEstimulos();
                break;
            case 4:
                migrarTabulador();
                break;
            case 5:
                migrarDepartamentos();
                break;
            case 6:
                migrarConceptos();
                break;
            case 7:
                migrarTablasImpuestos();
                break;
            default:
                System.out.println("Default");
        }
        System.out.println("Finish");
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
                    
                    System.out.println("" + sql);
                    
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
                        sql = "INSERT INTO Departamentos VALUES (default, '" + depto + "', '" + nombre +"');";
                        
                        System.out.println("" + sql);
                        
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
    
    
    private static void migrarTablasImpuestos() {
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver postgresDriver = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(postgresDriver);

            Connection connOracle = DriverManager.getConnection(CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_DEVELOPMENT, "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPostgres = connPostgres.createStatement()) {
                
                // Vaciar tabulador
                String sql = "DELETE FROM TarifaAnual;"; 
                stmtPostgres.executeUpdate(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE tarifaanual_id_seq RESTART WITH 1;";
                stmtPostgres.executeUpdate(sql);
                
                String sqlTabla = "select * from NO05s";
                ResultSet rsOra = stmtOra.executeQuery(sqlTabla);
                Double lim_inf = 0.01;
                while (rsOra.next()) {
                    Double lim_sup = rsOra.getDouble("NO05S_LS");
                    if (lim_sup > 0.00) {
                        Double cuota = rsOra.getDouble("NO05S_CUOTA");
                        Double perc = rsOra.getDouble("NO05S_PERC");

                        // insertar el registro en Tabulador
                        sql = "INSERT INTO TarifaAnual VALUES (DEFAULT, " + lim_inf + ", " + lim_sup + ", " + cuota + ", " + perc  +");";
                        stmtPostgres.executeUpdate(sql);

                        lim_inf = Math.floor((lim_sup + 0.01) * 100) / 100;
                    }
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
                            case "'I'": // Investigación
                                idTipoAntiguedad = 1;
                                break;
                            case "'S'": // Sin Antigüedad
                                idTipoAntiguedad = 0;
                                break;
                            case "'A'": // Administrativa
                                idTipoAntiguedad = 2;
                                break;
                        }
                    }
                    int idSede = 0;
                    String ubicacion = stringQuoted(rsOra.getString("NO01_UBICACION"));
                    if (null != ubicacion) {
                        switch (ubicacion) {
                            case "'1'": // chi
                                idSede = 0;
                                break;
                            case "'2'": // Juárez (no hay)
                                idSede = 3;
                                break;
                            case "'3'": // mty
                                idSede = 1;
                                break;
                            case "'4'": //dgo
                                idSede = 2;
                                break;
                        }
                    }

                    String fechaIniContrato = makeDate(rsOra.getString("NO01_INI_CONTRATO"));
                    String fechaFinContrato = makeDate(rsOra.getString("NO01_FIN_CONTRATO"));

                    int idTipoContrato = rsOra.getInt("NO01_TIPO_CONTRATO");
                    int idTipoSni = 0;
                    String sni = stringQuoted(rsOra.getString("NO01_SNI"));
                    if ("NO APLICA".contains(sni.trim())) {
                        idTipoSni = 0;
                    } else if ("CANDIDATO".contains(sni.trim())) {
                        idTipoSni = 1;
                    } else if ("'NIVEL I'".equals(sni.trim())) {
                        idTipoSni = 2;
                    } else if ("'NIVEL II'".equals(sni.trim())) {
                        idTipoSni = 3;
                    } else if ("'NIVEL III'".equals(sni.trim())) {
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

                    /** FECHAS **/
                    // NO01_FECHA_ING           fecha ingreso al Cimav
                    // NO01_FECHA_CAL           la misma que NO01_FECHA_ING (no se usa)
                    // NO01_FECHA_ING_FED       la usada para la PAnt. Para todo es la misma;
                    //                          excepto para unos cuantos CYTs (Villafañe, Alarcon, etc.)
                    // NO01_FECHA_DP            Creo que la ultima vez que se ingreso a un Centro 
                    //                          Para la gran mayoria es NO01_FECHA_ING
                    //                          Exception: Alarcon tiene ¿cuando se fue a Durango?
                    //                          Villafañe desde que entró al Cimav
                    // NO01_FECHA_APF           Creoq que como la de Pant, la 1era (NO01_FECHA_ING_FED)
                    
                    String fechaAntiguedad = makeDate(rsOra.getString("NO01_FECHA_ING_FED"));
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

                    /*
                    String sqlMigrarEmpleado = "INSERT INTO empleados VALUES ( default, "
                            + cveEmp + ", " + consecutivo + ", " + idDepto + ", " + idStatus + ", " + curp + ", " + rfc + ", " + imss + ", " + idProyecto + ", " + cuentaBanco
                            + ", " + urlPhoto + ", " + name + ", " + apellidoPAt + ", " + apellidoMat + ", " + idGrupo + ", " + nombre + ", " + idTabulador + ", " + idClinica
                            + ", " + cuentaCimav + ", " + idBanco + ", " + idSede + ", " + idTipoEmpleado + ", " + idTipoContrato
                            + ", " + fechaIng + ", " + fechaIniContrato + ", " + fechaFinContrato + ", " + fechaBaja + ", " + idTipoAntiguedad + ", " + fechaAntiguedad
                            + ", " + idTipoSni + ", " + numSni + ", " + fechaSNI 
                            + ", NULL, " + fechaNacimiento + ", " + idSexo + ", " + idEdoCivil + ", " + dirCalle + ", " + dirColonia + ", " + dirCP + ", " + telefono + ", " + emailPersonal
                            + " );";
                    */

                    String estimulos = "0.00";
                    
                    String sqlMigrarEmpleado = "INSERT INTO empleados VALUES ( default, "
                            + idStatus + ", " + urlPhoto + ", " + cuentaCimav + ", " + idTabulador + ", " + idGrupo + ", " + idDepto + ", " + idSede + ", "
                            + fechaAntiguedad + ", " + estimulos + ", " + idTipoAntiguedad + ", " + consecutivo + ", " + curp + ", " + rfc + ", " + imss + ", " 
                            + idProyecto + ", " + cuentaBanco + ", " + apellidoPAt + ", " + apellidoMat + ", " + nombre + ", " + idClinica + ", " + idBanco + ", "
                            + idTipoEmpleado + ", " + idTipoContrato + ", " + fechaIng + ", " + fechaIniContrato + ", " + fechaFinContrato + ", " + fechaBaja + ", "   
                            + idTipoSni + ", " + numSni + ", " + fechaSNI + ", NULL, "  + fechaNacimiento + ", " + idSexo + ", " + idEdoCivil + ", " 
                            + dirCalle + ", " + dirColonia + ", " + dirCP + ", " + telefono + ", " + emailPersonal + ","
                            + name + ", " + cveEmp
                        + " );";
                            
                    System.out.println("" + sqlMigrarEmpleado);
                    
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

                    System.out.println("" + sqlUpdateJefe);
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

    private static void migrarEstimulos() {

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
                // 19 es la constante de los estimulos
                String sql = "SELECT no02_cve_emp, no02_conce, no02_fijo FROM no02 where no02_conce = '19'";
                ResultSet rsOra = stmtOra.executeQuery(sql);

                while (rsOra.next()) {
                    String cveEmp = rsOra.getString("no02_cve_emp").trim();
                    String estimulos = rsOra.getString("no02_fijo").trim();

                    String sqlUpdateJefe = "UPDATE empleados SET estimulos_productividad = " + estimulos + " WHERE code = '" + cveEmp + "';";

                    System.out.println("" + sqlUpdateJefe);
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
                
//                // Vaciar departamentos
                String sql = ""; 
                sql = "DELETE FROM Conceptos;"; 
                stmtPostgres.executeUpdate(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE conceptos_id_seq RESTART WITH 1;";
                stmtPostgres.executeUpdate(sql);
                
                // sacar lista ordenada de Conceptos (Percepciones y Deducciones) capturables
                String sqlConceptos = "select c.* from no04 c where c.no04_tmovto in ('E', 'P', 'D') order by c.no04_conce";
                //String sqlConceptos = "select c.* from no04 c order by c.no04_conce";
                try (ResultSet rsOra = stmtOra.executeQuery(sqlConceptos)) {
                    while (rsOra.next()) {
                        String code = rsOra.getString("NO04_CONCE").trim();
                        String nombre = rsOra.getString("NO04_nombre");
                        String tipoConcepto = rsOra.getString("NO04_tmovto").trim();
                        String tipoMovimiento = stringQuoted("C");
                        
                        code = Strings.padStart(code, 5, '0');
                        code = stringQuoted(code);
                        nombre = stringQuoted(nombre);
                        tipoConcepto = tipoConcepto.contains("E") ? "R" : tipoConcepto;
                        tipoConcepto = stringQuoted(tipoConcepto);
                        
                        // insertar el registro en Conceptos
                        
                        // Si el Depto es vacio, se lanza un Trigger
                        // Si el Depto No es vacio, se inserta directo.
                        sql = "INSERT INTO Conceptos VALUES (default, " + code + ", " + nombre + ", " + tipoConcepto + ", " + tipoMovimiento + ", true);";
                        
                        System.out.println("" + sql);
                        
                        stmtPostgres.executeUpdate(sql);
                    }
                    
                    // Extras Conceptos
                    // Internas
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'BG' , 'BASE GRAVABLE', 'I', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'BE' , 'BASE EXENTA', 'I', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SUD' , 'SUELDO DIARIO', 'I', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDF' , 'SALARIO DIARIO FIJO', 'I', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDV' , 'SALARIO DIARIO VARIABLE', 'I', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDC' , 'SALARIO DIARIO COTIZADO', 'I', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDCT' , 'SALARIO DIARIO COTIZADO TOPADO', 'I', 'C', true);");
                    // Repercuciones
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'E3SMG' , 'EXCEDENTE 3SMG', 'R', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'PED' , 'PRESTACIONES EN DINERO', 'R', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'GMYP' , 'GTOS MEDICOS Y PENSION', 'R', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'IYV' , 'INVALIDEZ Y VIDA', 'R', 'C', true);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'CYV' , 'CESANTIA Y VEJEZ', 'R', 'C', true);");
                    
                    
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


/*

-- UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 999.91 FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = '00398';

UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00012';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00409';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00432';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00304';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00444';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00435';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00408';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00437';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00412';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00357';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00441';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00362';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00361';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00388';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	0	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00445';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	18.45	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00443';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	24.68	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00311';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	34.92	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00395';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	34.92	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00431';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	34.92	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00433';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	36.05	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00434';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	36.98	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00344';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	36.98	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00416';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	38.84	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00423';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	39.93	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00410';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	40.11	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00426';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	41.5	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00414';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	42.29	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00352';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	43.75	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00324';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	44.1	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00305';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	44.5	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00294';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	45.71	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00256';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	50.55	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00298';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	50.66	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00063';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	51.61	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00121';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	52.58	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00068';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	53.94	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00001';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	64.97	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00439';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	66.7	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00372';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	67.59	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00320';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	69.06	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00066';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	72.87	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00341';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	74.43	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00406';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	80.59	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00053';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	81.6	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00265';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	83.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00392';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	83.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00399';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	85.72	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00424';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	86.13	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00333';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	86.52	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00328';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	86.61	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00182';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	91.93	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00419';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	92.68	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00391';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	92.96	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00154';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	93.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00314';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	93.8	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00286';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	94.29	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00263';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	94.72	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00274';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	94.97	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00397';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	95.9	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00185';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	101.8	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00099';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	108.15	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00100';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	108.27	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00271';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	109.77	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00400';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	109.77	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00373';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	109.77	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00422';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	109.77	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00356';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	109.77	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00374';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	111.98	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00009';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	112.19	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00077';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	113.13	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00104';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	116.41	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00018';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	116.95	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00427';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	119.4	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00350';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	119.84	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00267';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	122.41	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00398';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	122.41	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00355';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	124.57	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00151';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	124.57	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00177';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	124.64	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00004';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	129.21	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00335';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	135.3	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00317';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	136.26	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00322';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	137.92	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00329';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	140.63	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00276';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	140.88	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00273';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	141.56	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00287';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	143.82	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00266';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	145.64	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00176';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	145.64	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00183';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	147.81	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00208';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	151.15	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00384';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	151.15	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00428';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	151.15	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00402';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	151.19	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00035';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	152.38	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00096';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	152.43	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00171';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	152.43	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00175';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	153.46	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00161';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	154.74	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00150';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	156.54	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00136';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	157.08	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00327';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	157.08	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00193';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	158.04	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00017';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	158.21	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00204';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	164.23	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00039';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	166.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00364';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	166.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00369';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	166.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00376';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	166.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00403';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	168.08	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00092';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	168.17	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00156';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	174.67	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00279';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	179.81	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00349';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	183.55	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00277';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	186.72	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00307';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	188.99	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00284';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	190.54	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00340';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	191.27	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00337';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	194.14	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00300';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	194.36	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00315';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	204.8	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00191';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	220.57	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00188';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	224.7	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00199';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	224.7	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00438';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	225.99	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00094';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	232.64	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00048';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	240.04	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00041';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	241.7	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00283';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	242.54	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00069';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	243.1	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00278';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	249.45	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00026';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	258.62	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00086';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	259.94	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00142';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	265.27	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00429';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	265.45	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00008';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	266.02	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00326';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	267.49	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00293';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	268.76	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00202';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	276.82	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00145';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	277.01	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00071';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	295.54	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00038';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	296.16	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00064';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	302.34	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00089';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	314.29	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00002';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	318.35	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00090';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	324.79	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00172';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	333.67	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00023';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	352.54	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00343';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	358.52	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00081';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	368.06	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00128';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	373.39	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00058';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	381.23	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00159';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	390.02	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00275';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	407.57	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00413';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	408.99	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00076';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	409.4	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00404';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	410.59	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00036';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	421.59	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00430';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	445.87	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00251';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	455.56	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00106';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	460.69	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00170';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	463.1	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00224';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	473.63	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00348';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	482.07	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00411';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	484.53	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00312';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	484.88	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00309';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	491.44	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00381';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	499.61	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00149';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	499.86	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00122';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	543.71	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00289';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	559.38	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00370';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	561.09	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00084';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	563.55	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00073';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	571.36	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00281';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	575.27	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00080';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	583.93	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00054';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	589.41	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00334';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	636.55	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00091';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	674.74	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00421';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	727.73	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00351';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	728.87	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00359';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	733.96	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00420';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	735.49	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00179';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	735.66	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00014';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	739.55	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00087';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	741.49	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00147';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	781.91	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00211';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	790.47	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00225';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	799.97	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00323';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	809.57	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00098';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	856.12	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00291';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	880.15	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00217';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	903.25	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00245';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	910.46	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00165';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	986.62	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00134';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	1018.15	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00316';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	1123.59	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00219';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	1186.06	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00103';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	1958.44	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00187';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	2082.71	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00290';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	2118.68	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00043';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	3115.41	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00375';
UPDATE empleadoquincenal SET sdi_variable_bimestre_anterior = 	12745.62	FROM empleados  WHERE empleadoquincenal.id_empleado = empleados.id and empleados.code = 	'00306';				

*/