*&---------------------------------------------------------------------*
*& Report ZAWS_LOAD_TEST
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
REPORT zaws_load_test.

TABLES : zdependency_view ,
         ddldependency    .

TYPES : BEGIN OF ty_ddlnames,
          mainview TYPE dd26s-tabname,
          level1   TYPE dd26s-tabname,
          level2   TYPE dd26s-tabname,
          level3   TYPE dd26s-tabname,
          level4   TYPE dd26s-tabname,
          level5   TYPE dd26s-tabname,
          level6   TYPE dd26s-tabname,
          level7   TYPE dd26s-tabname,
          level8   TYPE dd26s-tabname,
          level9   TYPE dd26s-tabname,
          level10  TYPE dd26s-tabname,
          level11  TYPE dd26s-tabname,
          level12  TYPE dd26s-tabname,
          coments  TYPE char60,
        END OF ty_ddlnames.

TYPES : BEGIN OF ty_table_view ,
          tabname  TYPE dd02l-tabname,
          viewname TYPE dd26s-viewname,
        END OF ty_table_view .

TYPES : BEGIN OF ty_dupplicate_names ,
          tabname TYPE ddldependency-ddlname,
          ddtext  TYPE string,
        END OF ty_dupplicate_names.

TYPES : BEGIN OF ty_only_tables ,
          only_table TYPE dd26s-tabname,
        END OF ty_only_tables .

DATA : gs_query_table     TYPE dd02l-tabname,
       gs_table_view      TYPE ty_table_view,
       gs_duplicate_names TYPE ty_dupplicate_names.


DATA : gt_out_table        TYPE dd02l-tabname,
       gt_dupplicate_names
                      TYPE STANDARD TABLE OF ty_dupplicate_names,
       gt_options          TYPE STANDARD TABLE OF /sapds/rfc_db_opt,
       gt_field            TYPE STANDARD TABLE OF rfc_db_fld,
       gt_tblout128        TYPE STANDARD TABLE OF /sapds/tab128,
       gt_tblout512        TYPE STANDARD TABLE OF /sapds/tab512,
       gt_tblout2048       TYPE STANDARD TABLE OF /sapds/tab2048,
       gt_tblout8192       TYPE STANDARD TABLE OF /sapds/tab8192,
       gt_tblout30000      TYPE STANDARD TABLE OF /sapds/tab30k,
       gt_table_view       TYPE STANDARD TABLE OF ty_table_view.

DATA : gw_ddtext TYPE string,
       gw_end    TYPE i.

DATA : gw_filename TYPE string,
       gw_string   TYPE string.

DATA : blob_size  TYPE i,
       lv_xstring TYPE xstring,
*       data       TYPE STANDARD TABLE OF sdokcntasc,
*       data_bin   TYPE STANDARD TABLE OF sdokcntbin.
        data       TYPE STANDARD TABLE OF ZSDOKCNTBIN,
        data_bin   TYPE STANDARD TABLE OF ZSDOKCNTBIN.


DATA: lr_bucket TYPE REF TO zlnkecl_aws_s3_bucket.
DATA: lr_cx_aws_s3 TYPE REF TO zlnkecx_aws_s3.

DATA: lv_filename TYPE string,
      lv_folder   TYPE string.
DATA: lv_content TYPE xstring.
DATA: lv_msg TYPE string.
DATA: lv_xml TYPE string.
DATA: lv_http_status TYPE i.

SELECT-OPTIONS : s_name FOR ddldependency-ddlname  NO INTERVALS.
PARAMETERS: p_dbg AS CHECKBOX.
PARAMETERS: p_bucket TYPE zlnkebucket-bucket LOWER CASE.
*PARAMETERS     : outfile  TYPE string MODIF ID dis,
*                 p_file   TYPE char1 AS CHECKBOX DEFAULT 'X',
*                 p_tabdis TYPE char1 AS CHECKBOX.
*                 p_file as CHECKBOX ."
*
*AT SELECTION-SCREEN ON s_name.
*
*  SELECT SINGLE * FROM  zdependency_view
*         INTO  @DATA(lt_zdependency_view)
*         WHERE ddlname IN @s_name[].
*  IF sy-subrc IS NOT INITIAL.
** Dummy
*  ENDIF.

INITIALIZATION.

*  outfile = '/tmp/.txt'.

AT SELECTION-SCREEN OUTPUT.

  LOOP AT SCREEN.

    IF screen-group1 = 'DIS'.
      screen-input = 0.
    ENDIF.

    MODIFY SCREEN.
  ENDLOOP.

START-OF-SELECTION .

*  DATA(gw_filename)   = '/tmp/BQ_SQL_SCRIPT.txt'.


*  TRANSLATE gw_filename TO LOWER CASE .




  LOOP AT s_name INTO DATA(gs_name).

    CLEAR : gs_query_table      ,
            gs_duplicate_names  .

    REFRESH : gt_field[]           ,
              gt_options[]         ,
              gt_tblout128[]       ,
              gt_tblout512[]       ,
              gt_tblout2048[]      ,
              gt_tblout8192[]      ,
              gt_tblout30000[]     ,
              gt_dupplicate_names[].

    gs_query_table = gs_name-low.

    TRY.

        CALL FUNCTION '/SAPDS/RFC_READ_TABLE2'
          EXPORTING
            query_table          = gs_query_table
            delimiter            = '|'
*           no_data              =
*           rowskips             =
*           rowcount             =
          IMPORTING
            out_table            = gt_out_table
          TABLES
            options              = gt_options
            fields               = gt_field
            tblout128            = gt_tblout128
            tblout512            = gt_tblout512
            tblout2048           = gt_tblout2048
            tblout8192           = gt_tblout8192
            tblout30000          = gt_tblout30000
          EXCEPTIONS
            table_not_available  = 1
            table_without_data   = 2
            option_not_valid     = 3
            field_not_valid      = 4
            not_authorized       = 5
            data_buffer_exceeded = 6
            OTHERS               = 7.
        IF sy-subrc <> 0.

          CONTINUE.

        ENDIF.

      CATCH cx_sy_dynamic_osql_semantics.
*DUmmy
    ENDTRY.

    IF gt_tblout128[] IS NOT INITIAL.
      gt_tblout512[] = gt_tblout128[].
    ELSEIF gt_tblout512[] IS NOT INITIAL.
      gt_tblout512[] = gt_tblout512[].
    ELSEIF gt_tblout2048[] IS NOT INITIAL.
      gt_tblout512[] = gt_tblout2048[].
    ELSEIF gt_tblout8192[] IS NOT INITIAL.
      gt_tblout512[] = gt_tblout8192[].
    ELSEIF gt_tblout30000[] IS NOT INITIAL.
      gt_tblout512[] = gt_tblout30000[].
    ENDIF.

    CALL FUNCTION 'SCMS_TEXT_TO_BINARY'
*     EXPORTING
*       FIRST_LINE            = 0
*       LAST_LINE             = 0
*       APPEND_TO_TABLE       = ' '
*       MIMETYPE              = ' '
*       ENCODING              =
      IMPORTING
        output_length = blob_size
      TABLES
        text_tab      = gt_tblout512
        binary_tab    = data_bin
      EXCEPTIONS
        failed        = 1
        OTHERS        = 2.
    IF sy-subrc <> 0.
* Implement suitable error handling here
    ENDIF.

    CALL FUNCTION 'SCMS_BINARY_TO_XSTRING'
      EXPORTING
        input_length = '104857600'
*       FIRST_LINE   = 0
*       LAST_LINE    = 0
      IMPORTING
        buffer       = lv_xstring
      TABLES
        binary_tab   = data_bin
      EXCEPTIONS
        failed       = 1
        OTHERS       = 2.
    IF sy-subrc <> 0.
* Implement suitable error handling here
    ENDIF.

* lv_filename = gs_query_table && |.csv| .
    lv_filename = gs_query_table && |.txt| .

    TRY.
        CREATE OBJECT lr_bucket
          EXPORTING
            i_bucket_name = p_bucket
            i_dbg         = p_dbg.

        CALL METHOD lr_bucket->put_object
          EXPORTING
            i_object_name      = lv_filename
            i_xcontent         = lv_xstring
            i_escape_url       = abap_false
          IMPORTING
            e_http_status      = lv_http_status
            e_response_content = lv_xml.

        IF lv_xml IS NOT INITIAL.
          zlnkecl_xml_utils=>show_xml_in_dialog( lv_xml ).
        ENDIF.

        IF lv_http_status = zlnkecl_http=>c_status_200_ok.
          CONCATENATE 'File ' lv_filename ' created successfully'
                 INTO lv_msg RESPECTING BLANKS.
        ELSE.
          CONCATENATE 'File ' lv_filename ' could not be created'
                 INTO lv_msg RESPECTING BLANKS.
        ENDIF.
        CONDENSE lv_msg.
        WRITE:/ lv_msg.

      CATCH zlnkecx_aws_s3 INTO lr_cx_aws_s3.
        lv_msg = lr_cx_aws_s3->get_text( ).
        WRITE:/ lv_msg.
    ENDTRY.


  ENDLOOP.

END-OF-SELECTION.
