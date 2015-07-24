DECLARE
  match_count INTEGER;
-- Type the owner of the tables you are looking at
  v_owner VARCHAR2(255) :='ALMACEN';

-- Type the data type you are look at (in CAPITAL)
-- VARCHAR2, NUMBER, etc.
  v_data_type VARCHAR2(255) :='CHAR';

-- Type the string you are looking at
  v_search_string VARCHAR2(400) := '%CALDERON OCHOA%';

BEGIN
  
  FOR t IN (SELECT table_name, column_name FROM all_tab_cols where owner=v_owner and data_type = v_data_type) LOOP

    if (length(t.table_name) < 10) THEN
      EXECUTE IMMEDIATE 
      'SELECT COUNT(*) FROM '||t.table_name||' WHERE '||t.column_name||' like :1'
      INTO match_count
      USING v_search_string;

      IF match_count > 0 THEN
        dbms_output.put_line( t.table_name ||' '||t.column_name||' '||match_count );
      END IF;
    END IF;

  END LOOP;
END;
/
