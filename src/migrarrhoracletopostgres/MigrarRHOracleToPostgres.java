/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package migrarrhoracletopostgres;

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

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here

        try {
            Driver oracleDriver = new oracle.jdbc.driver.OracleDriver();
            Driver oraclePostgres = new org.postgresql.Driver();
            
            DriverManager.registerDriver(oracleDriver);
            DriverManager.registerDriver(oraclePostgres);

            Connection connOracle = DriverManager.getConnection("jdbc:oracle:thin:@//10.1.0.44:1521/cimav14XDB.netmultix.cimav.edu.mx", "almacen", "afrika");
            Connection connPostgres = DriverManager.getConnection("jdbc:postgresql://10.0.4.40:5432/rh_development", "rh_user", "rh_1ser");

            try (Statement stmtOra = connOracle.createStatement(); Statement stmtPost = connPostgres.createStatement()) {
                String sql = "SELECT e.* FROM NO01 e where e.NO01_STATUS != 'B'";
                ResultSet rsOra = stmtOra.executeQuery(sql);
                
                while(rsOra.next()){
                    String cveEmp = rsOra.getString("NO01_CVE_EMP").trim();
                    int consecutivo = Integer.parseInt(cveEmp);
                    String nomEmp = rsOra.getString("NO01_NOM_EMP").trim();
                    String cveDepto = rsOra.getString("NO01_depto").trim();
                    String rfc = rsOra.getString("NO01_RFC_EMP").trim();
                    String imss = rsOra.getString("NO01_CVE_IMSS").trim();
                    String curp = rsOra.getString("NO01_CURP").trim();
                    
                    String fechaIng = makeDate(rsOra.getString("NO01_FECHA_ING"));
                    
                    String categoria = rsOra.getString("NO01_CATEGORIA").trim();
                    String nomBanco = rsOra.getString("NO01_NOM_BANCO").trim();
                    int idBanco = nomBanco.contains("BANORTE") ? 0 : 9; 
                    String cuentaBanco = rsOra.getString("NO01_CTA_BANCO").trim();
                    String regimen = rsOra.getString("NO01_REGIMEN").trim();
                    int idGrupo = 0;
                    if (regimen.contains("AYA")) {
                        idGrupo = 1;
                    } else if (regimen.contains("CYT")) {
                        idGrupo = 2;
                    } else if (regimen.contains("MMS")) {
                        idGrupo = 3;
                    } if (regimen.contains("HON")) {
                        idGrupo = 4;
                    }
                    int idTipoAntiguedad = 0;
                    String tipoAnt = rsOra.getString("NO01_TIPO_ANT").trim();
                    if (null != tipoAnt) switch (tipoAnt) {
                        case "I":
                            idTipoAntiguedad = 1;
                            break;
                        case "S":
                            idTipoAntiguedad = 0;
                            break;
                        case "A":
                            idTipoAntiguedad = 2;
                            break;
                    }
                    int idSede = 0;
                    String ubicacion = rsOra.getString("NO01_UBICACION").trim();
                    if (null != ubicacion) switch (ubicacion) {
                        case "1": // chi
                            idSede = 0;
                            break;
                        case "3": // mty
                            idSede = 1;
                            break;
                        case "4": //dgo
                            idSede = 2;
                            break;
                    }
                    
                    String fechaIniContrato = makeDate(rsOra.getString("NO01_INI_CONTRATO"));
                    String fechaFinContrato = makeDate(rsOra.getString("NO01_FIN_CONTRATO"));

                    
                    int idTipoContrato = rsOra.getInt("NO01_TIPO_CONTRATO");
                    int idTipoSni = 0;
                    String sni = rsOra.getString("NO01_SNI").trim();
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
                    String numSni = rsOra.getString("NO01_NUM_SNI").trim();
                    
                    String fechaSNI = makeDate(rsOra.getString("NO01_FECHA_SNI"));
                    
                    String numCredito = rsOra.getString("NO01_ID_CREDITO").trim();
                    String apellidoPAt = rsOra.getString("NO01_APELLIDO_PAT").trim();
                    String apellidoMat = rsOra.getString("NO01_APELLIDO_MAT").trim();
                    String nombre = rsOra.getString("NO01_NOMBRE").trim();
                    String proyecto = rsOra.getString("NO01_PROYECTO").trim();
                    String clinica = rsOra.getString("NO01_CLINICA").trim();
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
                    sql = "select * from departamentos d where d.codigo = '" + cveDepto.trim() + "';";
                    ResultSet rsPost = stmtPost.executeQuery(sql);
                    while (rsPost.next()) {
                        idDepto = rsPost.getInt("id");
                        codigoDepto = rsPost.getString("codigo");
                        nomDepto = rsPost.getString("nombre");
                    }                    
                    int idTabulador = 0;
                    String codeTabulador = "";
                    String nameTabulador = "";
                    sql = "select * from tabulador d where d.code = '" + categoria.trim() + "';";
                    rsPost = stmtPost.executeQuery(sql);
                    while (rsPost.next()) {
                        idTabulador = rsPost.getInt("id");
                        codeTabulador = rsPost.getString("code");
                        nameTabulador = rsPost.getString("name");
                    }                    

                    String cuentaCimav = rsOra.getString("NO01_EMAIL60");
                    cuentaCimav = cuentaCimav != null && !cuentaCimav.trim().isEmpty() ? cuentaCimav.split("@")[0] : "default";
                    
                    String fechaAntiguedad = makeDate(rsOra.getString("NO01_FECHA_CAL"));
                    fechaAntiguedad = fechaAntiguedad == null || fechaAntiguedad.trim().isEmpty() ? fechaIng : null;
                    
                    int idStatus = 0;
                    int idProyecto = 0;
                    String urlPhoto = "http://www.cimav.edu.mx/foto/" + cuentaCimav;
                    int idTipoEmpleado = 0;   
                    String fechaBaja = fechaFinContrato;
                    String name = "";
                    
                    cveEmp = "'" + cveEmp + "'";
                    curp = "'" + curp + "'";
                    rfc = "'" + rfc + "'";
                    imss = "'" + imss + "'";
                    cuentaBanco = "'" + cuentaBanco + "'";
                    urlPhoto = "'" + urlPhoto + "'";
                    name = "'" + name + "'";
                    apellidoPAt = "'" + apellidoPAt + "'";
                    apellidoMat = "'" + apellidoMat + "'";
                    nombre = "'" + nombre + "'";
                    numCredito = "'" + numCredito + "'";
                    cuentaCimav = "'" + cuentaCimav + "'";
                    numSni = "'" + numSni + "'";
                    
                    String sqlMigration = "INSERT INTO empleados VALUES ( default, " +
                        cveEmp + ", " + consecutivo  + ", " + idDepto + ", " + idStatus  + ", " + curp  + ", " + rfc + ", " + imss  + ", " + idProyecto  + ", " + cuentaBanco
                         + ", " + urlPhoto  + ", " + name  + ", " + apellidoPAt + ", " + apellidoMat + ", " + idGrupo  + ", "  + nombre + ", " + idTabulador  + ", " + idClinica
                         + ", " + (numCredito.trim().length() > 3)  + ", " + numCredito  + ", " + cuentaCimav  + ", " + idBanco  + ", " + idSede  + ", " + idTipoEmpleado  + ", " + idTipoContrato
                           + ", " + fechaIng  + ", " + fechaIniContrato  + ", " + fechaFinContrato  + ", "  + fechaBaja  + ", " + idTipoAntiguedad  + ", " + fechaAntiguedad
                          + ", " + idTipoSni  + ", " + numSni  + ", " + fechaSNI + 
                    " );";
                 
                    System.out.println("" + sqlMigration);
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
        if(fechaOra != null && !fechaOra.trim().isEmpty() && fechaOra.trim().length() == 8) {
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

}
