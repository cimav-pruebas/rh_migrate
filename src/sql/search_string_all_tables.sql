--/
DECLARE  
        -- Funciona en DbVisualizer Pro 9.2.7   TIENE QUE SER PRO
  ncount NUMBER;  
  vwhere VARCHAR2(1000) := '';  
  vselect VARCHAR2(1000) := ' select count(1) from ';  
  vsearchstr VARCHAR2(1000) := '%CAPTURAR SUELDO MENSUAL%';  
  vstmt VARCHAR2(1000) := '';  
  vquery VARCHAR(1000) := '';
BEGIN  
  FOR k IN (SELECT a.table_name  
                  ,a.column_name  
              FROM user_tab_cols a  
             WHERE a.table_name LIKE ('NO%') AND a.data_type LIKE '%CHAR%')  
  LOOP  
    vwhere := ' where ' || k.column_name || ' like :vsearchstr ';  
    vstmt := vselect || k.table_name || vwhere  ;
    --dbms_output.put_line(vstmt);
    EXECUTE IMMEDIATE vstmt  
      INTO ncount  
      USING vsearchstr;  
    IF (ncount > 0)  
    THEN  
        vquery := 'select ' || k.column_name || ' from ' || k.table_name || ' where ' || k.column_name || ' like ''' || vsearchstr || ''';';
        dbms_output.put_line(vquery);  
    END IF;  
  END LOOP;  
END;  
/