class ZCL_AWS_S3_ES_IUUC_REPL definition
  public
  final
  create public .

public section.

  interfaces IF_BADI_INTERFACE .
  interfaces IF_BADI_IUUC_REPL_OLO_EXIT .
protected section.
private section.
ENDCLASS.



CLASS ZCL_AWS_S3_ES_IUUC_REPL IMPLEMENTATION.


  method IF_BADI_IUUC_REPL_OLO_EXIT~ADJUST_TABLE_STRUCTURE.
  endmethod.


  METHOD if_badi_iuuc_repl_olo_exit~write_data_for_initial_load.

    DATA:
      lv_tabname_target TYPE tabname,
      ls_return         TYPE bapiret2.

    DATA : gw_ddtext TYPE string,
           gw_end    TYPE i.

    DATA : gw_filename TYPE string,
           gw_string   TYPE string.

    DATA : blob_size  TYPE i,
           lv_xstring TYPE xstring,
           data       TYPE STANDARD TABLE OF sdokcntasc,
           data_bin   TYPE STANDARD TABLE OF sdokcntbin.

    DATA: lr_bucket TYPE REF TO zlnkecl_aws_s3_bucket.
    DATA: lr_cx_aws_s3 TYPE REF TO zlnkecx_aws_s3.

    DATA: lv_filename TYPE string,
          lv_folder   TYPE string.
    DATA: lv_content TYPE xstring.
    DATA: lv_msg TYPE string.
    DATA: lv_xml TYPE string.
    DATA: lv_http_status TYPE i,
          gs_tblout30000 TYPE /sapds/tab30k,
          gt_tblout30000 TYPE STANDARD TABLE OF /sapds/tab30k.

    DATA : gs_DFIES_TAB TYPE dfies,
           gt_DFIES_TAB TYPE dfies_tab.

    DATA : lv_string TYPE string.
    DATA : lv_X030L_WA    TYPE  x030l,
           lv_DDOBJTYPE   TYPE  dd02v-tabclass,
           lv_DDOBJNAME   TYPE  ddobjname,
           lv_DFIES_WA    TYPE  dfies,
           lv_LINES_DESCR TYPE  ddtypelist,
           lv_timestamp   TYPE timestampl.

    GET TIME STAMP FIELD lv_timestamp.

    FIELD-SYMBOLS:
      <lt_table>           TYPE STANDARD TABLE,
      <ls_table>           TYPE any,
      <ls_string>          TYPE any,
      <ls_table_w_content> TYPE if_badi_iuuc_repl_olo_exit=>gty_s_table_w_payload_il.

    CLEAR ev_error_code.
    CLEAR et_return.

*    BREAK-POINT.
    LOOP AT it_table_w_content ASSIGNING <ls_table_w_content>.

      lv_DDOBJNAME = <ls_table_w_content>-tabname_source.

      CALL FUNCTION 'DDIF_FIELDINFO_GET'
        EXPORTING
          tabname        = lv_DDOBJNAME
          langu          = sy-langu
        IMPORTING
          x030l_wa       = lv_X030L_WA
          ddobjtype      = lv_DDOBJTYPE
          dfies_wa       = lv_DFIES_WA
          lines_descr    = lv_LINES_DESCR
        TABLES
          dfies_tab      = gt_DFIES_TAB
        EXCEPTIONS
          not_found      = 1
          internal_error = 2
          OTHERS         = 3.
      IF sy-subrc = 0.
        CLEAR : lv_string ,
                gs_DFIES_TAB.

        LOOP AT gt_DFIES_TAB INTO gs_DFIES_TAB.
          lv_string = lv_string && '|' && gs_DFIES_TAB-fieldname .
        ENDLOOP.
        gs_tblout30000 = lv_string.
        APPEND gs_tblout30000 TO gt_tblout30000.
      ENDIF.


      ASSIGN <ls_table_w_content>-payload->* TO <lt_table>.

      IF <lt_table>[] IS NOT INITIAL.
        LOOP AT <lt_table> ASSIGNING <ls_table>.

          CLEAR : lv_string ,
                  gs_DFIES_TAB.
          LOOP AT gt_DFIES_TAB INTO gs_DFIES_TAB.

            ASSIGN COMPONENT gs_DFIES_TAB-fieldname
                              OF STRUCTURE <ls_table>
                                    TO <ls_string> .

            lv_string = lv_string && '|' && <ls_string> .
          ENDLOOP.

          gs_tblout30000 = lv_string.
          APPEND gs_tblout30000 TO gt_tblout30000.
        ENDLOOP.
      ENDIF.

*      gt_tblout30000 = <lt_table>.

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
          text_tab      = gt_tblout30000
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
*         FIRST_LINE   = 0
*         LAST_LINE    = 0
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
      lv_filename = <ls_table_w_content>-tabname_source && '_' &&
                    lv_timestamp
                    && |.txt| .

      TRY.
          CREATE OBJECT lr_bucket
            EXPORTING
              i_bucket_name = 'dataanlytics01'
              i_dbg         = 'X'.

          CALL METHOD lr_bucket->put_object
            EXPORTING
              i_object_name      = lv_filename
              i_xcontent         = lv_xstring
              i_escape_url       = abap_false
            IMPORTING
              e_http_status      = lv_http_status
              e_response_content = lv_xml.

*        IF lv_xml IS NOT INITIAL.
*          zlnkecl_xml_utils=>show_xml_in_dialog( lv_xml ).
*        ENDIF.

*        IF lv_http_status = zlnkecl_http=>c_status_200_ok.
*          CONCATENATE 'File ' lv_filename ' created successfully'
*                 INTO lv_msg RESPECTING BLANKS.
*        ELSE.
*          CONCATENATE 'File ' lv_filename ' could not be created'
*                 INTO lv_msg RESPECTING BLANKS.
*        ENDIF.
*        CONDENSE lv_msg.
*        WRITE:/ lv_msg.

        CATCH zlnkecx_aws_s3 INTO lr_cx_aws_s3.
*        lv_msg = lr_cx_aws_s3->get_text( ).
*        WRITE:/ lv_msg.
      ENDTRY.

    ENDLOOP.

  ENDMETHOD.


  METHOD if_badi_iuuc_repl_olo_exit~write_data_for_repl.

    DATA:
      lv_tabname_target TYPE tabname,
      ls_return         TYPE bapiret2.

    DATA : gw_ddtext TYPE string,
           gw_end    TYPE i.

    DATA : gw_filename TYPE string,
           gw_string   TYPE string.

    DATA : blob_size  TYPE i,
           lv_xstring TYPE xstring,
           data       TYPE STANDARD TABLE OF sdokcntasc,
           data_bin   TYPE STANDARD TABLE OF sdokcntbin.

    DATA: lr_bucket TYPE REF TO zlnkecl_aws_s3_bucket.
    DATA: lr_cx_aws_s3 TYPE REF TO zlnkecx_aws_s3.

    DATA: lv_filename TYPE string,
          lv_folder   TYPE string.
    DATA: lv_content TYPE xstring.
    DATA: lv_msg TYPE string.
    DATA: lv_xml TYPE string.
    DATA: lv_http_status TYPE i,
          gs_tblout30000 TYPE /sapds/tab30k,
          gt_tblout30000 TYPE STANDARD TABLE OF /sapds/tab30k.

    DATA : gs_DFIES_TAB TYPE dfies,
           gt_DFIES_TAB TYPE dfies_tab.

    DATA : lv_string TYPE string.
    DATA : lv_X030L_WA    TYPE  x030l,
           lv_DDOBJTYPE   TYPE  dd02v-tabclass,
           lv_DDOBJNAME   TYPE  ddobjname,
           lv_DFIES_WA    TYPE  dfies,
           lv_LINES_DESCR TYPE  ddtypelist.

    FIELD-SYMBOLS:
      <lt_table>           TYPE STANDARD TABLE,
      <ls_table>           TYPE any,
      <ls_string>          TYPE any,
      <ls_table_w_content> TYPE LINE OF if_badi_iuuc_repl_olo_exit=>gty_t_table_w_payload.

    DATA: lv_operation TYPE c LENGTH 8,
          lv_lines     TYPE i,
          lv_timestamp TYPE timestampl.

    GET TIME STAMP FIELD lv_timestamp.

    CLEAR ev_error_code.
    CLEAR et_return.

    LOOP AT it_table_w_content
         ASSIGNING <ls_table_w_content>.

      ASSIGN <ls_table_w_content>-payload->* TO <lt_table>.

      lv_tabname_target = <ls_table_w_content>-tabname_source.
      lv_operation = <ls_table_w_content>-operation.
      DESCRIBE TABLE <lt_table> LINES lv_lines.

      IF <lt_table>[] IS NOT INITIAL.
        lv_DDOBJNAME = <ls_table_w_content>-tabname_source.

        CALL FUNCTION 'DDIF_FIELDINFO_GET'
          EXPORTING
            tabname        = lv_DDOBJNAME
            langu          = sy-langu
          IMPORTING
            x030l_wa       = lv_X030L_WA
            ddobjtype      = lv_DDOBJTYPE
            dfies_wa       = lv_DFIES_WA
            lines_descr    = lv_LINES_DESCR
          TABLES
            dfies_tab      = gt_DFIES_TAB
          EXCEPTIONS
            not_found      = 1
            internal_error = 2
            OTHERS         = 3.
        IF sy-subrc = 0.
          CLEAR : lv_string ,
                  gs_DFIES_TAB.

          LOOP AT gt_DFIES_TAB INTO gs_DFIES_TAB.
            lv_string = lv_string && '|' && gs_DFIES_TAB-fieldname .
          ENDLOOP.
          gs_tblout30000 = lv_string.
          APPEND gs_tblout30000 TO gt_tblout30000.
        ENDIF.

        CASE lv_operation.
          WHEN if_badi_iuuc_repl_olo_exit=>gc_operation_insert.

            IF <lt_table>[] IS NOT INITIAL.
              LOOP AT <lt_table> ASSIGNING <ls_table>.

                CLEAR : lv_string ,
                        gs_DFIES_TAB.
                LOOP AT gt_DFIES_TAB INTO gs_DFIES_TAB.

                  ASSIGN COMPONENT gs_DFIES_TAB-fieldname
                                    OF STRUCTURE <ls_table>
                                          TO <ls_string> .

                  lv_string = lv_string && '|' && <ls_string> .
                ENDLOOP.

                gs_tblout30000 = lv_string.
                APPEND gs_tblout30000 TO gt_tblout30000.
              ENDLOOP.
            ENDIF.

            lv_filename = if_badi_iuuc_repl_olo_exit=>gc_operation_insert && '_' &&
                          <ls_table_w_content>-tabname_source  && '_' &&
                          lv_timestamp && |.txt| .

          WHEN if_badi_iuuc_repl_olo_exit=>gc_operation_update.

            IF <lt_table>[] IS NOT INITIAL.
              LOOP AT <lt_table> ASSIGNING <ls_table>.

                CLEAR : lv_string ,
                        gs_DFIES_TAB.
                LOOP AT gt_DFIES_TAB INTO gs_DFIES_TAB.

                  ASSIGN COMPONENT gs_DFIES_TAB-fieldname
                                    OF STRUCTURE <ls_table>
                                          TO <ls_string> .

                  lv_string = lv_string && '|' && <ls_string> .
                ENDLOOP.

                gs_tblout30000 = lv_string.
                APPEND gs_tblout30000 TO gt_tblout30000.
              ENDLOOP.
            ENDIF.

            lv_filename = if_badi_iuuc_repl_olo_exit=>gc_operation_update && '_' &&
                          <ls_table_w_content>-tabname_source  && '_' &&
                          lv_timestamp && |.txt| .

          WHEN if_badi_iuuc_repl_olo_exit=>gc_operation_delete.

            IF <lt_table>[] IS NOT INITIAL.
              LOOP AT <lt_table> ASSIGNING <ls_table>.

                CLEAR : lv_string ,
                        gs_DFIES_TAB.
                LOOP AT gt_DFIES_TAB INTO gs_DFIES_TAB.

                  ASSIGN COMPONENT gs_DFIES_TAB-fieldname
                                    OF STRUCTURE <ls_table>
                                          TO <ls_string> .

                  lv_string = lv_string && '|' && <ls_string> .
                ENDLOOP.

                gs_tblout30000 = lv_string.
                APPEND gs_tblout30000 TO gt_tblout30000.
              ENDLOOP.
            ENDIF.

            lv_filename = if_badi_iuuc_repl_olo_exit=>gc_operation_delete && '_' &&
                          <ls_table_w_content>-tabname_source  && '_' &&
                          lv_timestamp && |.txt| .

          WHEN OTHERS.

            " Error handling
            ls_return-type = 'E'.
            ls_return-message = 'Invalid operation'.
            APPEND ls_return TO et_return.
            ev_error_code = 0.

        ENDCASE.

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
            text_tab      = gt_tblout30000
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
*           FIRST_LINE   = 0
*           LAST_LINE    = 0
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

        TRY.
            CREATE OBJECT lr_bucket
              EXPORTING
                i_bucket_name = 'dataanlytics01'
                i_dbg         = 'X'.

            CALL METHOD lr_bucket->put_object
              EXPORTING
                i_object_name      = lv_filename
                i_xcontent         = lv_xstring
                i_escape_url       = abap_false
              IMPORTING
                e_http_status      = lv_http_status
                e_response_content = lv_xml.

          CATCH zlnkecx_aws_s3 INTO lr_cx_aws_s3.
        ENDTRY.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  method IF_BADI_IUUC_REPL_OLO_EXIT~WRITE_DATA_FOR_REPL_CLUSTER.

  endmethod.
ENDCLASS.
