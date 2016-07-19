\set libfile '\''`pwd`'/R/vertica.udx.util.R\''
CREATE OR REPLACE LIBRARY vertica_dplyr AS :libfile LANGUAGE 'R';
CREATE OR REPLACE TRANSFORM FUNCTION vertica_function_executor AS LANGUAGE 'R' NAME 'vertica_function_executor.factory' LIBRARY vertica_dplyr fenced;
