class ZCL_GA_SQL definition
  public
  create public .

public section.

  methods CLOSE_CONNECTION .
  methods EXECUTE
    importing
      value(IP_STMT) type STRING
      value(IR_RESULT) type ref to DATA optional
    returning
      value(R_OK) type ABAP_BOOL .
  methods QUERY
    importing
      value(IP_QUERY) type STRING
    changing
      value(OR_RESULT) type ref to DATA
    returning
      value(R_OK) type ABAP_BOOL .
  methods CONSTRUCTOR
    importing
      value(IP_DBCO) type DBCON_NAME
      value(IO_LOGGER) type ref to ZCL_ALOG_MSG_LOGGER_BASE optional
    raising
      CX_SQL_EXCEPTION .
  class-methods GET_FIELDS
    importing
      value(IP_STR_NAME) type STRING optional
      value(IR_STR_REF) type ref to DATA optional
      value(IP_DATA) type ANY optional
    returning
      value(R_FIELDS) type STRING .
  class-methods GET_SQL_VALUE
    importing
      value(IP_ANY) type ANY
    returning
      value(R_SQL) type STRING .
protected section.
private section.

  data O_LOGGER type ref to ZCL_ALOG_MSG_LOGGER_BASE .
  data O_CONN_DB type ref to CL_SQL_CONNECTION .
  data V_DBCO type DBCON_NAME .

  methods LOG_EXCEPTION
    importing
      value(IX_EX) type ref to CX_ROOT .
  methods LOG_ERROR
    importing
      value(IV_TEXT) type CSEQUENCE .
  methods LOG_INFO
    importing
      value(IV_TEXT) type CSEQUENCE .
ENDCLASS.



CLASS ZCL_GA_SQL IMPLEMENTATION.


  METHOD close_connection.

    CHECK o_conn_db IS BOUND AND NOT o_conn_db->is_closed( ).

    o_conn_db->close( ).
  ENDMETHOD.


  METHOD constructor.
    IF ip_dbco IS INITIAL.
      RAISE EXCEPTION TYPE cx_sql_exception.
    ENDIF.
    v_dbco = ip_dbco.

    IF io_logger IS BOUND.
      o_logger ?= io_logger.
    ENDIF.
    o_conn_db = cl_sql_connection=>get_connection( v_dbco ).
    log_info( |Connected to DB: { v_dbco  } | ).
  ENDMETHOD.


  METHOD execute.
    DATA: lo_prepstmt_ref TYPE REF TO cl_sql_prepared_statement,
          ld_string       TYPE string.
    DATA: lx_sql_error   TYPE REF TO cx_sql_exception,
          lx_param_error TYPE REF TO cx_parameter_invalid..

    TRY.
        r_ok = abap_false.
        IF o_conn_db->is_closed( ).
          o_conn_db = cl_sql_connection=>get_connection( v_dbco ).
        ENDIF.
        lo_prepstmt_ref = o_conn_db->prepare_statement( CONV #( ip_stmt ) ).
        IF ir_result IS SUPPLIED.
          lo_prepstmt_ref->set_param_struct( ir_result ).
        ENDIF.
        lo_prepstmt_ref->execute_update( ).
        lo_prepstmt_ref->close( ).
        o_conn_db->commit( ).
      CATCH cx_sql_exception INTO lx_sql_error.

        log_error( |Error SQL al ejecutar { ip_stmt } en { v_dbco }| ).
        log_exception( lx_sql_error ).
        o_conn_db->close( ).

        RETURN.
      CATCH cx_parameter_invalid INTO lx_param_error.

        log_error( |Error SQL al ejecutar { ip_stmt } en { v_dbco }| ).
        log_exception( lx_param_error ).
        o_conn_db->close( ).

        RETURN.
    ENDTRY.
    r_ok = abap_true.
  ENDMETHOD.


  METHOD get_fields.
    r_fields = '*'.
    CHECK ip_str_name IS NOT INITIAL OR ir_str_ref IS BOUND OR ip_data IS SUPPLIED.
*    DATA lo_type TYPE REF TO cl_abap_typedescr.
    DATA(lo_type) = COND #(
                   WHEN ip_str_name IS NOT INITIAL THEN cl_abap_typedescr=>describe_by_name( ip_str_name )
                   WHEN ip_data IS SUPPLIED THEN cl_abap_typedescr=>describe_by_data( ip_data )
                   WHEN ir_str_ref IS BOUND THEN cl_abap_typedescr=>describe_by_data_ref( ir_str_ref )

                   ).
    DATA lo_str TYPE REF TO cl_abap_structdescr.
    CASE lo_type->type_kind.
*
      WHEN cl_abap_typedescr=>typekind_struct1 OR cl_abap_typedescr=>typekind_struct2.
        lo_str = CAST cl_abap_structdescr( lo_type ).
      WHEN cl_abap_typedescr=>typekind_table.
        lo_str = CAST cl_abap_structdescr( CAST cl_abap_tabledescr( lo_type )->get_table_line_type( ) ).
    ENDCASE.

*    DATA(lo_str) = CAST cl_abap_structdescr( lo_type ).
    DATA(lt_comp) = lo_str->get_components( ).
*    loop at lt_comp into data(lo_comp).
*      IF lo_comp-TYPE->kind =
*      endif.
*    endloop.
    DATA lt_fields TYPE TABLE OF string.
    LOOP AT lt_comp INTO DATA(lo_comp).

      CASE TYPE OF lo_comp-type.
        WHEN TYPE cl_abap_elemdescr.
          APPEND |[{ lo_comp-name }]| TO lt_fields.
        WHEN OTHERS.
      ENDCASE.
    ENDLOOP.
    CHECK lt_fields IS NOT INITIAL.
    CLEAR r_fields.
    LOOP AT lt_fields INTO DATA(ls_field).
      r_fields = |{ r_fields }{ ls_field }|.
      AT LAST.
        EXIT.
      ENDAT.
      r_fields = |{ r_fields },|.
    ENDLOOP.
  ENDMETHOD.


  METHOD get_sql_value.
    DATA(ld_type_kind) = cl_abap_datadescr=>get_data_type_kind( ip_any ).
    CASE ld_type_kind.
      WHEN cl_abap_typedescr=>typekind_date. "fecha
        DATA: ld_date TYPE sy-datum.
        ld_date = ip_any.
"B23024 7/11/2025 dos cambios:
"si la fecha viene vacia o inicial insertamos nulo en intranet
"sino la fecha la ponemos en formato iso => AAAA-MM-DD
         if ld_date is initial OR ld_date = '00000000'.
           r_sql = |NULL|.
         else.
           r_sql = |'{ ld_date date = iso }'|.
           "r_sql = |'{ ld_date(4) }-{ ld_date+4(2) }-{ ld_date+6(2) }'|.
         endif.
        RETURN.
      WHEN cl_abap_typedescr=>typekind_time. "hora
        DATA: ld_time TYPE sy-uzeit.
        ld_time = ip_any.
        r_sql = |'{ ld_time TIME = USER }'|.
        RETURN.
      WHEN cl_abap_typedescr=>typekind_char OR
               cl_abap_typedescr=>typekind_csequence OR
               cl_abap_typedescr=>typekind_string. "tipo caracter

        "r_sql =  CONV #( |{ ip_any ALPHA = OUT }| ) .


        r_sql = |{ condense( ip_any ) }|.
"B23024 7/10/2025 cambiamos las comillas simples por dobles
"para que el sql no de error
        REPLACE ALL OCCURRENCES OF ''''
        IN r_sql WITH ''''''.

        r_sql = |'{ r_sql }'|.
        "r_sql = |'{ condense( ip_any ) }'|.

        RETURN.
      WHEN cl_abap_typedescr=>typekind_decfloat OR
         cl_abap_typedescr=>typekind_decfloat16 OR
         cl_abap_typedescr=>typekind_decfloat34 OR
         cl_abap_typedescr=>typekind_float OR
         cl_abap_typedescr=>typekind_int OR
         cl_abap_typedescr=>typekind_int1 OR
         cl_abap_typedescr=>typekind_int8 OR
         cl_abap_typedescr=>typekind_int2 OR
         cl_abap_typedescr=>typekind_num OR
         cl_abap_typedescr=>typekind_numeric OR
         cl_abap_typedescr=>typekind_packed. "numero
        r_sql = |{ CONV f( ip_any ) }|.
        RETURN.
      WHEN OTHERS. "no se me ocurre.
        r_sql = CONV #( ip_any ).
        RETURN.

    ENDCASE.
  ENDMETHOD.


  METHOD LOG_ERROR.
    TRY.
        CHECK o_logger IS BOUND.
        o_logger->error( iv_text ).

      CATCH cx_root.

    ENDTRY.
  ENDMETHOD.


  METHOD log_exception.
    TRY.
        CHECK o_logger IS BOUND.
        o_logger->exception( ix_ex ).
      CATCH cx_root.

    ENDTRY.
  ENDMETHOD.


  METHOD log_info.
    TRY.
        CHECK o_logger IS BOUND.
        o_logger->info( iv_text ).

      CATCH cx_root.

    ENDTRY.
  ENDMETHOD.


  METHOD query.
    DATA:
      ld_stmt_ref TYPE REF TO cl_sql_statement,
      ld_res_ref  TYPE REF TO cl_sql_result_set,
      ld_string   TYPE string.
    DATA: lx_sql_error   TYPE REF TO cx_sql_exception,
          lx_param_error TYPE REF TO cx_parameter_invalid.

    TRY .
        r_ok = abap_false.
        IF o_conn_db->is_closed( ).
          o_conn_db = cl_sql_connection=>get_connection( v_dbco ).
        ENDIF.
        ld_stmt_ref = o_conn_db->create_statement( ).
        ld_res_ref = ld_stmt_ref->execute_query( CONV #( ip_query ) ).
        ld_res_ref->set_param_table( or_result ).
        ld_res_ref->next_package( ).
        ld_res_ref->close( ).
      CATCH cx_sql_exception INTO lx_sql_error.
        log_exception( lx_sql_error ).
        o_conn_db->close( ).
        RETURN.
      CATCH cx_parameter_invalid INTO lx_param_error.
        log_exception( lx_sql_error ).
        o_conn_db->close( ).
        RETURN.
    ENDTRY.
    r_ok = abap_true.
  ENDMETHOD.
ENDCLASS.
