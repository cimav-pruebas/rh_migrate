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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author juan.calderon
 */
public class MigrarRHOracleToPostgres {

    public static String NETMULTIX_CIMAV_15_XDB = "jdbc:oracle:thin:@//10.1.0.44:1521/cimavXDB.netmultix.cimav.edu.mx";
    //public static String RH_DEVELOPMENT = "jdbc:postgresql://10.0.4.40:5432/rh_development";
    public static String RH_PRODUCTION = "jdbc:postgresql://10.0.4.40:5432/rh_production";
    //public static String RH_DEVELOPMENT = "jdbc:postgresql://localhost:5432/rh_development";

    /*
    SELECT MAX(id)+1 FROM nomina;
    CREATE SEQUENCE nomina_id_seq MINVALUE 446;
    ALTER TABLE nomina ALTER id SET DEFAULT nextval('nomina_id_seq');
    ALTER SEQUENCE nomina_id_seq OWNED BY nomina.id
    */
    
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

        // 4,5,7,0,1,6,2,3
        int opcion = 3;

        switch (opcion) {
            case 0:
                vaciarEmpleados();
                break;
            case 1:
                // TODO Checar fechas (el uso horario).
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
            case 8:
                insertarMovimientosMasivos();
                break;
            case 9:
                insertarMovimientosCruzRoja();
                insertarMovimientosGtosMAyores();
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

            Connection connOracle = DriverManager.getConnection(NETMULTIX_CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

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

            Connection connOracle = DriverManager.getConnection(NETMULTIX_CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

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

            Connection connOracle = DriverManager.getConnection(NETMULTIX_CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

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

            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

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

        int conta = 0;

        /* Generar todos registros de Empleados desde N001 */
        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connOracle = DriverManager.getConnection(NETMULTIX_CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPostgress = connPostgres.createStatement()) {
                
                // VACIAR NominaQuincenal
                //String sql = "DELETE FROM NominaQuincenal;";
                String sql = "DELETE FROM Nomina;";
                stmtPostgress.execute(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE nomina_id_seq RESTART WITH 1;";
                stmtPostgress.execute(sql);

                // VACIAR Empleados
                sql = "DELETE FROM Empleados;";
                stmtPostgress.execute(sql);
                
                // reiniciar seq
                sql = "ALTER SEQUENCE empleados_id_seq RESTART WITH 1;";
                stmtPostgress.execute(sql);
                
                // Leer empleados de NetMultix; excepto Bajas. NO01_FECHA_SAL es la fecha de baja.
                sql = "SELECT e.* FROM NO01 e where e.NO01_STATUS != 'B' or (e.NO01_STATUS = 'B' and e.NO01_FECHA_SAL > '20160101')"; // AND e.NO01_CVE_EMP like '%0076%'";
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
                    
                    conta++;
                }
                
            } catch (Exception e2) {
                migrarJefes = false;
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
                connOracle.close();
                System.out.println("FISNHS>>> " + conta + " empleados");
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

            Connection connOracle = DriverManager.getConnection(NETMULTIX_CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

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

            Connection connOracle = DriverManager.getConnection(NETMULTIX_CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

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

            Connection connOracle = DriverManager.getConnection(NETMULTIX_CIMAV_15_XDB, "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

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
                        Integer integra = rsOra.getInt("NO04_IMSS");
                        Integer grava = rsOra.getInt("NO04_EXENTO");
                        boolean suma = rsOra.getInt("NO04_IMPRIMIR") == 1;
                        
                        code = Strings.padStart(code, 5, '0');
                        code = stringQuoted(code);
                        nombre = stringQuoted(nombre);
                        tipoConcepto = tipoConcepto.contains("E") ? "R" : tipoConcepto;
                        tipoConcepto = stringQuoted(tipoConcepto);
                        switch (grava) {
                            case 0:
                                grava = 0;
                                break;
                            case 9999:
                                grava = 1;
                                break;
                            case 15:
                            case 30:
                                grava = 2;
                                break;
                            default:
                                grava = 0;
                                break;
                        }
                        
                        // Grava
                        // 0->Grava         0->Grava: FAG, TEG, Sueldo, Gratificacion
                        // 1->Excenta       9999->Exenta: Ayuda guardería, FAE, TEE
                        // 2->Parcial       15-> Prima vacacional
                        //                  30-> Aguinaldo
                        // Integra
                        // 0->NoIntegra     0->NoIntegra, Mondero, Ayudas, Abonos, Apoyos
                        // 1->Integra       SiIntegra, Sueldo, Aguinaldo, Carga, Primas,  
                        // 2->Variado       Variado  Despensa, Dif. Tiempo Extra, Gratifi, Remanente, Monedero Anual
                        
                        // sacar uno por uno los que suman
                        
                        // insertar el registro en Conceptos
                        
                        // Si el Depto es vacio, se lanza un Trigger
                        // Si el Depto No es vacio, se inserta directo.
                        sql = "INSERT INTO Conceptos VALUES (default, " + code + ", " + nombre + ", " + tipoConcepto + ", " + tipoMovimiento + "," +  suma +", " + integra + "," + grava + ");";
                        
                        System.out.println("" + sql);
                        
                        stmtPostgres.executeUpdate(sql);
                    }
                    
                    // Extras Conceptos
                    // Internas
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'BG' , 'BASE GRAVABLE', 'I', 'C', true, 0, 0);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'BE' , 'BASE EXENTA', 'I', 'C', true, 0, 0);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SUD' , 'SUELDO DIARIO', 'I', 'C', true, 0, 0);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDF' , 'SALARIO DIARIO FIJO', 'I', 'C', true, 0, 0);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDV' , 'SALARIO DIARIO VARIABLE', 'I', 'C', true, 0, 0);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDC' , 'SALARIO DIARIO COTIZADO', 'I', 'C', true, 0, 0);");
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SDCT' , 'SALARIO DIARIO COTIZADO TOPADO', 'I', 'C', true, 0, 0);");
                    // Repercuciones
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'E3SMG' , 'EXCEDENTE 3SMG', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'PED' , 'PRESTACIONES EN DINERO', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'GMYP' , 'GTOS MEDICOS Y PENSION', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'IYV' , 'INVALIDEZ Y VIDA', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'CYV' , 'CESANTIA Y VEJEZ', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'CFIJA' , 'CUOTA FIJA', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'RIESGOT' , 'RIESGO DE TRABAJO', 'R', 'C', true, 0, 0);");  //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'GYPS' , 'GUARDERIAS Y PRESTACIONES SOCIALES', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'SEGRET' , 'SEGURO DE RETIRO', 'R', 'C', true, 0, 0);"); //
                    stmtPostgres.executeUpdate("INSERT INTO Conceptos VALUES (default, 'INFONA' , 'INFONAVIT', 'R', 'C', true, 0, 0);"); //
                    
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
    
    private static void insertarMovimientosMasivos() {

        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

            try (Statement stmtPostgress = connPostgres.createStatement()) {

                String codes = "("
                        + "'00333', "
                        + "'00344', "
                        + "'00416', "
                        + "'00265', "
                        + "'00320', "
                        + "'00063', "
                        + "'00099', "
                        + "'00434', "
                        + "'00286', "
                        + "'00298', "
                        + "'00009', "
                        + "'00312', "
                        + "'00145', "
                        + "'00306', "
                        + "'00343', "
                        + "'00149', "
                        + "'00136', "
                        + "'00147', "
                        + "'00018', "
                        + "'00266', "
                        + "'00224', "
                        + "'00350', "
                        + "'00290', "
                        + "'00039', "
                        + "'00171', "
                        + "'00202', "
                        + "'00276', "
                        + "'00410', "
                        + "'00364', "
                        + "'00373', "
                        + "'00217', "
                        + "'00430', "
                        + "'00054', "
                        + "'00422', "
                        + "'00337', "
                        + "'00315', "
                        + "'00329', "
                        + "'00058', "
                        + "'00278', "
                        + "'00335', "
                        + "'00283', "
                        + "'00150', "
                        + "'00281', "
                        + "'00128', "
                        + "'00080', "
                        + "'00419', "
                        + "'00087', "
                        + "'00176', "
                        + "'00314', "
                        + "'00334', "
                        + "'00090', "
                        + "'00094', "
                        + "'00284', "
                        + "'00183', "
                        + "'00106', "
                        + "'00156', "
                        + "'00307', "
                        + "'00277', "
                        + "'00289', "
                        + "'00170', "
                        + "'00159', "
                        + "'00322', "
                        + "'00012', "
                        + "'00304', "
                        + "'00362', "
                        + "'00388' "
                        + ")";
                String sql = "SELECT id FROM empleados WHERE code in " + codes;
                ResultSet rs = stmtPostgress.executeQuery(sql);
                List<String> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getString("id").trim());
                }
                /*
                for(String id : ids) {
                    sql = "INSERT INTO movimientos VALUES (default, " + id + ", 115, 15.00, 1, 15.00, 15.00, true, 0.00); " + "\n"
                          + "INSERT INTO movimientos VALUES (default, " + id + ", 91, 15.00, 1, 0.00, 0.00, true, 0.00);";
                    stmtPostgress.execute(sql);
                }
                */

            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void insertarMovimientosCruzRoja() {

        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

            try (Statement stmtPostgress = connPostgres.createStatement()) {

                String codes = "("
                        + "'00185', "
                        + "'00256', "
                        + "'00265', "
                        + "'00035', "
                        + "'00036', "
                        + "'00038', "
                        + "'00043', "
                        + "'00150', "
                        + "'00279', "
                        + "'00081', "
                        + "'00084', "
                        + "'00092', "
                        + "'00090', "
                        + "'00170', "
                        + "'00004', "
                        + "'00068' "
                        + ")";
                String sql = "SELECT id FROM empleados WHERE code in " + codes;
                ResultSet rs = stmtPostgress.executeQuery(sql);
                List<String> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getString("id").trim());
                }
                for(String id : ids) {
                    sql = "INSERT INTO movimientos VALUES (default, " + id + ", 108, 10.00, 1, 10.00, 10.00, true, 0.00); ";
                    stmtPostgress.execute(sql);
                }

            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void insertarMovimientosGtosMAyores() {

        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();

            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connPostgres = DriverManager.getConnection(RH_PRODUCTION, "rh_user", "rh_1ser");

            try (Statement stmtPostgress = connPostgres.createStatement()) {

                String codes = "("
                        + "'00063', "
                        + "'00294', "
                        + "'00348', "
                        + "'00002', "
                        + "'00306', "
                        + "'00161', "
                        + "'00300', "
                        + "'00317', "
                        + "'00147', "
                        + "'00172', "
                        + "'00219', "
                        + "'00266', "
                        + "'00224', "
                        + "'00039', "
                        + "'00375', "
                        + "'00316', "
                        + "'00204', "
                        + "'00054', "
                        + "'00337', "
                        + "'00315', "
                        + "'00329', "
                        + "'00351', "
                        + "'00283', "
                        + "'00420', "
                        + "'00150', "
                        + "'00128', "
                        + "'00211', "
                        + "'00069', "
                        + "'00081', "
                        + "'00086', "
                        + "'00428', "
                        + "'00092', "
                        + "'00142', "
                        + "'00091', "
                        + "'00089', "
                        + "'00356', "
                        + "'00094', "
                        + "'00096', "
                        + "'00284', "
                        + "'00291', "
                        + "'00103', "
                        + "'00104', "
                        + "'00165', "
                        + "'00277', "
                        + "'00411', "
                        + "'00225', "
                        + "'00159', "
                        + "'00403', "
                        + "'00304' "
                        + ")";
                String sql = "SELECT id FROM empleados WHERE code in " + codes;
                ResultSet rs = stmtPostgress.executeQuery(sql);
                List<String> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getString("id").trim());
                }
                for(String id : ids) {
                    sql = "INSERT INTO movimientos VALUES (default, " + id + ", 107, 100.00, 1, 100.00, 100.00, true, 0.00); ";
                    stmtPostgress.execute(sql);
                }

            } catch (Exception e2) {
                System.out.println(">>> " + e2.getMessage());
            } finally {
                connPostgres.close();
            }

        } catch (SQLException ex) {
            Logger.getLogger(MigrarRHOracleToPostgres.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
