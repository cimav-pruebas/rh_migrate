/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package migrarrhoracletopostgres;

/**
 *
 * @author juan.calderon
 */
public enum EClinica {
    CLINICA_001(12, "001", "CLINICA 001"),
    CLINICA_002(8, "002", "CLINICA 002"),
    CLINICA_003(11, "003", "CLINICA 003"),
    CLINICA_004(9, "004", "CLINICA 004"),
    CLINICA_008(4, "008", "CLINICA 008"),
    CLINICA_010(3, "010", "CLINICA 010"),
    CLINICA_015(14, "015", "CLINICA 015"),
    CLINICA_019(13, "019", "CLINICA 019"),
    CLINICA_027(5, "027", "CLINICA 027"),
    CLINICA_029(10, "029", "CLINICA 029"),
    CLINICA_031(15, "031", "CLINICA 031"),
    CLINICA_032(1, "032", "CLINICA 032"),
    CLINICA_033(2, "033", "CLINICA 033"),
    CLINICA_044(6, "044", "CLINICA 044"),
    CLINICA_058(7, "058", "CLINICA 058");

    private int id;
    private String code;
    private String name;

    private EClinica(int id, String code, String name) {
        this.id = id;
        this.code = code;
        this.name = name;
    }

    public static EClinica get(int id) {
        EClinica result = EClinica.CLINICA_044; // default
        for (EClinica value : EClinica.values()) {
            if (value.getId() == id) {
                result = value;
                break;
            }
        }
        return result;
    }

    public static Integer getId(String code) {
        EClinica result = EClinica.CLINICA_044; // default
        if (code == null || code.trim().isEmpty()) {
            return result.getId();
        }
        for (EClinica value : EClinica.values()) {
            if (value.getCode().equals(code.trim())) {
                result = value;
                break;
            }
        }
        return result.getId();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
 