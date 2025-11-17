class ZCL_GA_DB_CONN definition
  public
  create public .

public section.

  class-data O_SQL type ref to ZCL_GA_SQL .
  class-data GO_LOGGER type ref to ZCL_ALOG_BAL_LOGGER_GA .

  class-methods MODIFY_TABLE
    importing
      !IP_TABLE type STRING
      !IT_KEYS type ABAP_TABLE_KEYDESCR_TAB
      !IP_WITH_TIMESTAMP type ABAP_BOOL default ABAP_FALSE
      !IS_BEEN_DELETED type ABAP_BOOL default ABAP_FALSE
    changing
      !IT_TABLE type TABLE .
  methods CONSTRUCTOR
    importing
      !IGO_LOGGER type ref to ZCL_ALOG_BAL_LOGGER_GA .
  class-methods DELETE_ALL
    importing
      !IP_TABLE type STRING
    returning
      value(OP_RESULT) type ABAP_BOOL .
  class-methods CHECK_DB_TABLE
    importing
      !IP_TABLE type STRING
      !IO_TBDSC type ref to CL_ABAP_TABLEDESCR
    changing
      !HAS_TIMESTAMP type ABAP_BOOL
    returning
      value(OP_RESULT) type ABAP_BOOL .
  class-methods MODIFY_ROW
    importing
      !IP_TABLE type STRING
      !IT_KEYS type ABAP_TABLE_KEYDESCR_TAB
      !IP_WITH_TIMESTAMP type ABAP_BOOL default ABAP_FALSE
      !IS_BEEN_DELETED type ABAP_BOOL default ABAP_FALSE
    changing
      !IS_ROW type ref to DATA .
  class-methods GET_GO_LOGGER
    importing
      value(IGO_LOGGER) type ref to ZCL_ALOG_BAL_LOGGER_GA optional .
  class-methods GET_SQL_VALUE
    importing
      value(IP_VALUE) type ANY
    returning
      value(R_SQL) type STRING .
  PROTECTED SECTION.
private section.

  constants C_INITIAL_DATE type SY-DATUM value '19000101' ##NO_TEXT.
  constants C_DEV_CONNECTION type DBCON_NAME value 'GANDBDESINT01' ##NO_TEXT.
  constants C_PRO_CONNECTION type DBCON_NAME value 'GANDBINT01' ##NO_TEXT.
  class-data D_DBCON type DBCON_NAME .

  class-methods INSERT_TABLE
    importing
      !IP_TABLE type STRING
    changing
      !IT_TABLE type TABLE
    returning
      value(OP_RESULT) type ABAP_BOOL .
  class-methods IS_DIFFERENT
    importing
      !IR_SAP type ref to DATA
      !IR_DB type ref to DATA
    returning
      value(OP_RESULT) type ABAP_BOOL .
  class-methods CHECK_LINE_EXISTS
    importing
      !IP_TABLE type STRING
      !IT_KEYS type ABAP_TABLE_KEYDESCR_TAB
      !IS_ROW type ref to DATA
    changing
      !OS_ROW type ref to DATA
    returning
      value(OP_EXISTS) type ABAP_BOOL .
  class-methods UPDATE_ROW
    importing
      !IP_TABLE type STRING
      !IT_KEYS type ABAP_TABLE_KEYDESCR_TAB
      !IP_WITH_TIMESTAMP type ABAP_BOOL default ABAP_FALSE
    changing
      !IS_ROW type ref to DATA
    returning
      value(OP_RESULT) type ABAP_BOOL .
  class-methods INSERT_ROW
    importing
      !IP_TABLE type STRING
    changing
      !IS_ROW type ref to DATA
    returning
      value(OP_RESULT) type ABAP_BOOL .
  class-methods GO_LOGGER_ERROR
    importing
      !ID_MSG type STRING .
ENDCLASS.



CLASS ZCL_GA_DB_CONN IMPLEMENTATION.


  METHOD CHECK_DB_TABLE.
    DATA: ld_stmt   TYPE string,
          ld_fields TYPE string,
          ld_from   TYPE string,
          ld_where  TYPE string,
          lo_ref    TYPE REF TO data,
          lt_table  TYPE TABLE OF string.

    op_result = abap_false.
    "chequeo existe la tabla
    ld_fields = 'TABLE_NAME'.
    ld_from = 'INFORMATION_SCHEMA.TABLES'.
    ld_where = | TABLE_NAME = '{ ip_table }'|.
    ld_stmt = |SELECT { ld_fields } FROM { ld_from } WHERE { ld_where }|.
    "
    GET REFERENCE OF lt_table INTO lo_ref.

    IF NOT ( o_sql->query( EXPORTING ip_query = CONV #( ld_stmt  )
    CHANGING or_result = lo_ref ) ).
      "go_logger->error( |Error Query FIELDS: { ld_fields }| ).
      "go_logger->error( |Error Query TABLE: { ld_from }| ).
      "go_logger->error( |Error Query WHERE: { ld_where }| ).

      go_logger_error( |Error Query FIELDS: { ld_fields }| ).
      go_logger_error( |Error Query TABLE: { ld_from }| ).
      go_logger_error( |Error Query WHERE: { ld_where }| ).
      RETURN.
    ENDIF.
    IF NOT line_exists( lt_table[ 1 ] ).
      go_logger_error( |TABLE { ip_table } NOT defined IN DB| ).
      RETURN.
    ENDIF.

    "check fields in table.
    DATA: lt_fields     TYPE TABLE OF string,
          lt_components TYPE abap_component_tab.
    ld_fields = 'upper( COLUMN_NAME )'.
    ld_from = 'INFORMATION_SCHEMA.COLUMNS'.
    ld_where = | TABLE_NAME = '{ ip_table }'|.
    ld_stmt = |SELECT { ld_fields } FROM { ld_from } WHERE { ld_where }|.
    GET REFERENCE OF lt_fields INTO lo_ref.
    IF NOT ( o_sql->query( EXPORTING ip_query = CONV #( ld_stmt  )
    CHANGING or_result = lo_ref ) ).
      go_logger_error( |Error Query FIELDS: { ld_fields }| ).
      go_logger_error( |Error Query TABLE: { ld_from }| ).
      go_logger_error( |Error Query WHERE: { ld_where }| ).
      RETURN.
    ENDIF.
    lt_components = CAST cl_abap_structdescr( io_tbdsc->get_table_line_type( ) )->get_components( ).

    LOOP AT lt_components INTO DATA(ls_component).
      IF NOT line_exists( lt_fields[ table_line = ls_component-name ] ).
        go_logger_error( |Error DB TABLE { ip_table } has NO FIELDS: { ls_component-name }| ).
        RETURN.
      ENDIF.
    ENDLOOP.

    IF line_exists( lt_fields[ table_line = 'TS' ] ).
      has_timestamp = abap_true.
    ENDIF.




    op_result = abap_true.
  ENDMETHOD.


  METHOD CHECK_LINE_EXISTS.
    DATA: ld_stmt   TYPE string,
          ld_field  TYPE string,
          ld_fields TYPE string,
          ld_value  TYPE string,
          ld_from   TYPE string,
          ld_where  TYPE string,

          lr_result TYPE REF TO data.
    DATA: lo_struc             TYPE REF TO cl_abap_structdescr,
          lo_table             TYPE REF TO cl_abap_tabledescr,
          lo_data              TYPE REF TO cl_abap_datadescr,
          lt_components        TYPE abap_component_tab,
          lt_components_fields TYPE abap_component_tab.
    FIELD-SYMBOLS: <lf_row>   TYPE any,
                   <lf_field> TYPE any.
    op_exists = abap_undefined.
    ASSIGN is_row->* TO <lf_row>.

    CHECK <lf_row> IS ASSIGNED.

    lo_struc ?= cl_abap_typedescr=>describe_by_data_ref( is_row ).

    lt_components_fields = lo_struc->get_components( ).
    LOOP AT lt_components_fields INTO DATA(ls_component).
      ld_field = ls_component-name.
      ASSIGN COMPONENT ld_field OF STRUCTURE <lf_row> TO <lf_field>.
      IF <lf_field> IS NOT ASSIGNED.
        RETURN.
      ENDIF.
      lo_data ?= cl_abap_typedescr=>describe_by_data( <lf_field> ).
      APPEND VALUE #( name = ld_field
      type = lo_data
      ) TO lt_components.
      CASE lo_data->type_kind.
*          ld_value
        WHEN cl_abap_typedescr=>typekind_date.
          ld_value = |isnull( CONVERT( varchar(8),{ ld_field }, 112), '{ c_initial_date }' )|.
        WHEN cl_abap_typedescr=>typekind_char OR
          cl_abap_typedescr=>typekind_csequence OR
          cl_abap_typedescr=>typekind_string.
          ld_value = | isnull( { ld_field }, '' )|.
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
          cl_abap_typedescr=>typekind_packed.
          ld_value = | isnull( { ld_field }, 0 )|.
        WHEN OTHERS.
          ld_value = |{ ld_field }|.
      ENDCASE.
      AT LAST.
        ld_fields = |{ ld_fields } { ld_value }|.
        EXIT.
      ENDAT.
      ld_fields = |{ ld_fields } { ld_value },|.
    ENDLOOP.
    CLEAR ld_value.
    LOOP AT it_keys[ is_primary = 'X' ]-components INTO DATA(ls_key).
      ld_field = ls_key-name.
      ASSIGN COMPONENT ld_field OF STRUCTURE <lf_row> TO <lf_field>.
      IF <lf_field> IS NOT ASSIGNED.
        RETURN.
      ENDIF.
      lo_data ?= cl_abap_typedescr=>describe_by_data( <lf_field> ).
      CASE lo_data->type_kind.
        WHEN cl_abap_typedescr=>typekind_char OR
          cl_abap_typedescr=>typekind_csequence OR
          cl_abap_typedescr=>typekind_string OR
          cl_abap_typedescr=>typekind_date.
          ld_value = |'{ <lf_field> }'|.
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
          cl_abap_typedescr=>typekind_packed.
          ld_value = | { <lf_field> }|.
        WHEN OTHERS.
          RETURN.
      ENDCASE.
      AT LAST.
        ld_where = |{ ld_where } { ld_field } = { ld_value }|.
        EXIT.
      ENDAT.
      ld_where = |{ ld_where } { ld_field } = { ld_value } AND |.
    ENDLOOP.


    ld_from = ip_table.
    ld_stmt = |SELECT { ld_fields } FROM { ld_from } WHERE { ld_where }|.
    FIELD-SYMBOLS:<lf_table> TYPE table.

    lo_struc = cl_abap_structdescr=>create( lt_components ).
    lo_table = cl_abap_tabledescr=>create( lo_struc ).

    CREATE DATA lr_result TYPE HANDLE lo_table.
    IF NOT ( o_sql->query( EXPORTING ip_query = CONV #( ld_stmt )
    CHANGING or_result = lr_result ) ).
      go_logger_error( |Error Query FIELDS: { ld_fields }| ).
      go_logger_error( |Error Query WHERE: { ld_where }| ).
      RETURN.
    ENDIF.
    ASSIGN lr_result->* TO <lf_table>.
    CHECK <lf_table> IS ASSIGNED.
    op_exists = xsdbool( <lf_table> IS NOT INITIAL ).
    READ TABLE <lf_table> INDEX 1 REFERENCE INTO os_row.
  ENDMETHOD.


    METHOD CONSTRUCTOR.

    DATA: lx_sql_exception TYPE REF TO cx_sql_exception.
    TRY.

      GET_GO_LOGGER( iGo_logger ).

      d_dbcon = c_dev_connection.
      CALL FUNCTION 'PRGN_CHECK_SYSTEM_PRODUCTIVE'
      EXCEPTIONS
        client_is_productive = 1
        OTHERS               = 2.
      IF sy-subrc = 1.
        d_dbcon = c_pro_connection.
      ENDIF.
      go_logger->info( |Connecting TO DB: { d_dbcon } | ).

      o_sql = NEW zcl_ga_sql( ip_dbco = CONV #( d_dbcon ) io_logger = CAST #( go_logger ) ).
    CATCH cx_sql_exception INTO lx_sql_exception.
      go_logger->exception( lx_sql_exception ).
    ENDTRY.

  ENDMETHOD.


  METHOD DELETE_ALL.
    DATA: ld_stmt TYPE string,
          ld_from TYPE string.

    op_result = abap_false.

    ld_from = ip_table.
    ld_stmt = |DELETE FROM { ld_from }|.
    go_logger->info( |Deleting content FROM TABLE { ld_from }| ).
    o_sql->execute( EXPORTING ip_stmt = ld_stmt ).
  ENDMETHOD.


  method GET_GO_LOGGER.

  IF igo_logger IS BOUND.
    go_logger = igo_logger.

  ENDIF.
  endmethod.


  method GET_SQL_VALUE.


  endmethod.


  method GO_LOGGER_ERROR.
    if go_logger is BOUND.
      go_logger->error( id_msg ).

    endif.

  endmethod.


  METHOD INSERT_ROW.


    DATA: ld_stmt   TYPE string,
          ld_from   TYPE string,
          ld_fields TYPE string,
          ld_values TYPE string.
    DATA: lo_struct     TYPE REF TO cl_abap_structdescr,
          lt_components TYPE abap_component_tab,
          ls_component  TYPE abap_componentdescr.

    op_result = abap_undefined.

    lo_struct ?= cl_abap_datadescr=>describe_by_data_ref( is_row ).

    CHECK lo_struct IS BOUND.
    lt_components = lo_struct->get_components( ).
    CHECK lt_components IS NOT INITIAL.
    LOOP AT lt_components INTO ls_component." WHERE name <> 'MANAGER_SGD'.
      DATA(ld_name) = ls_component-name.
      AT FIRST.
        ld_fields = '('.
        ld_values = 'VALUES('.
      ENDAT.
      AT LAST.
        ld_fields = |{ ld_fields }[{ ld_name }])|.
        ld_values = |{ ld_values }?)|.
        EXIT.
      ENDAT.
      ld_fields = |{ ld_fields }[{ ld_name }],|.
      ld_values = |{ ld_values }?,|.

    ENDLOOP.
    ld_from = ip_table.
    ld_stmt = |INSERT INTO { ld_from } { ld_fields } { ld_values }|.

    IF NOT ( o_sql->execute( EXPORTING ip_stmt = ld_stmt
                                        ir_result = is_row )
    ).

      "go_logger->error( | INSERT AT { ld_from }| ).
      go_logger_error( | INSERT AT { ld_from }| ).
    ENDIF.
  ENDMETHOD.


  method INSERT_TABLE.

  DATA: ld_stmt   TYPE string,
        ld_from   TYPE string,
        ld_fields TYPE string,
        ld_values TYPE string.

  DATA: lo_struct     TYPE REF TO cl_abap_structdescr,
        lt_components TYPE abap_component_tab,
        ls_component  TYPE abap_componentdescr.
  data: ld_values_row type string.

  op_result = abap_undefined.


  if lines( it_table ) = 0.
    return.
  endif.

  lo_struct ?= cl_abap_datadescr=>describe_by_data( it_table[ 1 ] ).

  CHECK lo_struct IS BOUND.
  lt_components = lo_struct->get_components( ).
  CHECK lt_components IS NOT INITIAL.
  LOOP AT lt_components INTO ls_component." WHERE name <> 'MANAGER_SGD'.
    DATA(ld_name) = ls_component-name.
    AT FIRST.
      ld_fields = '('.

    ENDAT.
    AT LAST.
      ld_fields = |{ ld_fields }[{ ld_name }])|.

      EXIT.
    ENDAT.
    ld_fields = |{ ld_fields }[{ ld_name }],|.
  ENDLOOP.

  data: ld_cont_max type i,
        ld_contador type i.
  ld_cont_max = 999.
  ld_contador = 0.


  ld_values = | VALUES |.
  ld_from = ip_table.

  LOOP AT it_table ASSIGNING FIELD-SYMBOL(<ls_row>).
    ld_contador = ld_contador + 1.

    LOOP AT lt_components INTO ls_component.

      assign COMPONENT ls_component-name of STRUCTURE <ls_row> to FIELD-SYMBOL(<lf_field>).
      if <lf_field> is assigned. "ya tienes el contenido del valor del campo de la linea

        at first.
          ld_values_row = |({ zcl_ga_sql=>get_sql_value( <lf_field> ) }|.
          continue.
        endat.
        at last.
          ld_values_row = |{ ld_values_row }, { zcl_ga_sql=>get_sql_value( <lf_field> ) })|.
          exit.
        ENDAT.
        ld_values_row = |{ ld_values_row } , { zcl_ga_sql=>get_sql_value( <lf_field> ) }|.


      endif.
      UNASSIGN <lf_field>.
    endloop.
    at last.
      ld_values = |{ ld_values } { ld_values_row }|.
      exit.
    endat.


    if ld_contador = ld_cont_max.
      ld_values = |{ ld_values } { ld_values_row }|.
      ld_stmt = |INSERT INTO { ld_from } { ld_fields } { ld_values }|.
      IF NOT ( o_sql->execute( EXPORTING ip_stmt = ld_stmt
*                                     ir_result = it_table
        ) ).
        go_logger_error( | INSERT AT { ld_from }| ).
      ENDIF.


      ld_values = | VALUES |.
      ld_values_row = '('.
      ld_contador = 0.
    else.
      ld_values = |{ ld_values } { ld_values_row },|.
    endif.
  endloop.


  ld_stmt = |INSERT INTO { ld_from } { ld_fields } { ld_values }|.
  IF NOT ( o_sql->execute( EXPORTING ip_stmt = ld_stmt
*                                     ir_result = it_table
          ) ).
    go_logger_error( | INSERT AT { ld_from }| ).
  ENDIF.


  endmethod.


  METHOD IS_DIFFERENT.
    DATA: lo_struc_sap TYPE REF TO cl_abap_structdescr,
*          lo_struc_db  TYPE REF TO cl_abap_structdescr,
*          lt_comp_db   TYPE abap_component_tab,
          lt_comp_sap  TYPE abap_component_tab.
    FIELD-SYMBOLS: <lf_row_sap>   TYPE any,
                   <lf_field_sap> TYPE any,
                   <lf_row_db>    TYPE any,
                   <lf_field_db>  TYPE any.
    op_result = abap_true.

    CHECK ir_sap IS BOUND.
    ASSIGN ir_sap->* TO <lf_row_sap>.
    CHECK <lf_row_sap> IS ASSIGNED.

    CHECK ir_db IS BOUND.
    ASSIGN ir_db->* TO <lf_row_db>.
    CHECK <lf_row_db> IS ASSIGNED.

    lo_struc_sap ?= cl_abap_typedescr=>describe_by_data_ref( ir_sap ).
    lt_comp_sap = lo_struc_sap->get_components( ).

    op_result = abap_false.
    LOOP AT lt_comp_sap INTO DATA(ls_comp).
      ASSIGN COMPONENT ls_comp-name OF STRUCTURE <lf_row_sap> TO <lf_field_sap>.
      ASSIGN COMPONENT ls_comp-name OF STRUCTURE <lf_row_db> TO <lf_field_db>.

      IF <lf_field_sap> IS NOT ASSIGNED OR <lf_field_db> IS NOT ASSIGNED
      OR <lf_field_sap> <> <lf_field_db> .
        op_result = abap_true.
        EXIT.
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD MODIFY_ROW.
    DATA: lo_struct         TYPE REF TO cl_abap_structdescr,
          lt_components     TYPE abap_component_tab,
          ls_component      TYPE abap_componentdescr,
          ls_row_to_compare TYPE REF TO data.
    CHECK is_row IS NOT INITIAL AND ip_table IS NOT INITIAL.

    TRY.

        IF is_been_deleted = abap_false AND
        check_line_exists( EXPORTING ip_table = ip_table
          it_keys = it_keys
          is_row = is_row
        CHANGING os_row = ls_row_to_compare ).
          IF is_different( ir_sap = is_row ir_db = ls_row_to_compare ).
            "actualizar
            update_row( EXPORTING ip_table = ip_table
              it_keys = it_keys
              ip_with_timestamp = ip_with_timestamp
            CHANGING is_row = is_row ).
          ENDIF.
        ELSE.
          "insertar
          insert_row( EXPORTING ip_table = ip_table
          CHANGING is_row = is_row ).
        ENDIF.

      CATCH cx_root.

    ENDTRY.
*    IF NOT o_conn->is_closed( ).
*      o_conn->close( ).
*    ENDIF.
    "o_sql->close_connection( ).


  ENDMETHOD.


  method MODIFY_TABLE.

      CHECK it_table IS NOT INITIAL AND ip_table IS NOT INITIAL.

  TRY.

    IF is_been_deleted = abap_false.

      data: lo_row  TYPE REF TO DATA.
      data(lv_lines) = lines( it_table  ).
      data(ld_contador) = 0.
      data(ld_cont_max) = 1000.
      LOOP AT it_table REFERENCE INTO lo_row.
        data(ld_indice) =  sy-tabix.
        ld_contador = ld_contador + 1.
        if ld_contador = ld_cont_max .
          data(lv_percent) = ld_indice * 100 / lv_lines.
          CHECK sy-batch IS INITIAL.
          CALL FUNCTION 'SAPGUI_PROGRESS_INDICATOR'
          EXPORTING
            percentage = lv_percent
            TEXT = TEXT-002.
          ld_cont_max = ld_cont_max + 1000.
        endif.

        zga_cl_db_conn=>modify_row( EXPORTING ip_table = CONV #( ip_table )
          it_keys = it_keys
          ip_with_timestamp = ip_with_timestamp
          is_been_deleted = is_been_deleted
        CHANGING is_row = lo_row ).

       ENDLOOP.

    ELSE.
      "insertar
      insert_table( EXPORTING ip_table = ip_table
      CHANGING it_table = it_table ).
    ENDIF.

  CATCH cx_root.

  ENDTRY.


  endmethod.


  METHOD UPDATE_ROW.

    DATA: ld_stmt       TYPE string,
          ld_field      TYPE string,
          ld_fields     TYPE string,
          ld_value      TYPE string,
          ld_from       TYPE string,
          ld_where      TYPE string,
          lo_struc      TYPE REF TO cl_abap_structdescr,
          lo_data       TYPE REF TO cl_abap_datadescr,
          lt_components TYPE abap_component_tab.
    FIELD-SYMBOLS: <lf_row>   TYPE any,
                   <lf_field> TYPE any.
    op_result = abap_false.

    lo_struc ?= cl_abap_typedescr=>describe_by_data_ref( is_row ).
    lt_components = lo_struc->get_components( ).

    LOOP AT lt_components INTO DATA(ls_comp).
      ld_field = ls_comp-name.
      AT FIRST.
        ld_fields = 'SET'.
      ENDAT.
      AT LAST.
        ld_fields = |{ ld_fields } { ld_field } = ?|.
        IF ip_with_timestamp = abap_true.
          ld_fields = |{ ld_fields }, TS = (getdate()) |.
        ENDIF.
        EXIT.
      ENDAT.
      ld_fields = |{ ld_fields } { ld_field } = ?,|.
    ENDLOOP.


    ASSIGN is_row->* TO <lf_row>.
    IF <lf_row> IS NOT ASSIGNED.
      RETURN.
    ENDIF.
    LOOP AT it_keys[ is_primary = 'X' ]-components INTO DATA(ls_key).
      ld_field = ls_key-name.
      ASSIGN COMPONENT ld_field OF STRUCTURE <lf_row> TO <lf_field>.
      IF <lf_field> IS NOT ASSIGNED.
        RETURN.
      ENDIF.
      lo_data ?= cl_abap_typedescr=>describe_by_data( <lf_field> ).
      CASE lo_data->type_kind.
        WHEN cl_abap_typedescr=>typekind_char OR
          cl_abap_typedescr=>typekind_csequence OR
          cl_abap_typedescr=>typekind_string.
          ld_value = |'{ <lf_field> }'|.
        WHEN OTHERS.
          RETURN.
      ENDCASE.
      AT LAST.
        ld_where = |{ ld_where } { ld_field } = { ld_value }|.
        EXIT.
      ENDAT.
      ld_where = |{ ld_where } { ld_field } = { ld_value } AND |.
    ENDLOOP.

    ld_from = ip_table.
    ld_where = | WHERE { ld_where }|.
    ld_stmt = |UPDATE { ld_from } { ld_fields } { ld_where }|.
    IF NOT ( o_sql->execute( EXPORTING ip_stmt = ld_stmt  ir_result = is_row ) ).
      "go_logger->error( | UPDATE AT { ld_from } WHERE { ld_where }| ).
      go_logger_error( | UPDATE AT { ld_from } WHERE { ld_where }| ).
    ENDIF.

  ENDMETHOD.
ENDCLASS.
