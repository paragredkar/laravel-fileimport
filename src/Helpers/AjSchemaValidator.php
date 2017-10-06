<?php
namespace Ajency\Ajfileimport\Helpers;

use Illuminate\Support\Facades\DB;
use Log;

/**
 * Class to validate the csv data imported to temporary table, based on master/child table schema
 */
class AjSchemaValidator
{

    private $table_name;
    private $fields;
    private $primary_key;
    private $indexes;
    private $exists = false;
    private $errors = [];
    /**
     *To do
     * For a table,
     * Update the records on temp table with error on following :--
     * select records from temporary table whose field value are empty and which should be non null in the main table.
     * select records from temporary table whose field value not integer and which should be integer in the main table.
     * select records from temporary table whose field value not double/float and which should be double/float number in the main table.
     * select records from temporary tables  whose field value are having no of characters greater than the number of characters for corresponding field on main table
     * select records from the temporary tables whose fields should be unique, and there are multiple records with same value.
     */

    /*
     *Before importing data to master or child table, Based on table structure of master/child table validate each field by length or value in temporary table
     */

    public function __construct($tablename)
    {
        $this->table_name = $tablename;
    }

    public function validateField($field_name, $field_data, $loop_count)
    {

        Log::info("test log");
        $qry_index = "ALTER TABLE " . $this->table_name . ". ADD INDEX (" . $field_name . ")";

        try {
            DB::statement($qry_index);

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.

            $this->errors[] = $ex->getMessage();
        }

        $batchsize = config('ajimportdata.batchsize');

        $limit = $loop_count * $batchsize;

        $this->validateFieldLength($field_name, $field_data, $limit, $batchsize);
        $this->validateFieldType($field_name, $field_data, $limit, $batchsize);
    }

    public function validateFieldLength($field_name, $field_data, $limit, $batchsize)
    {

        Log::info('update query start');
        $temp_table_name = $this->table_name;

        if (!isset($field_data->maxlength) && !isset($field_data->maxvalue)) {
            return;
        }

        $qry = "UPDATE " . $this->table_name . " SET  aj_error_log = ( CASE ";

        $error_length = ", Length exceeds the field length";
        $error_value  = ", value of field exceeds max value";

        $maxlength_cond       = " length(" . $field_name . ") > " . $field_data->maxlength;
        $maxvalue_cond        = isset($field_data->maxvalue) ? " " . $field_name . "  > " . $field_data->maxvalue : "";
        $errorlog_isnull_cond = " (aj_error_log IS NULL  or aj_error_log ='' or aj_error_log='NULL') ";

        if (isset($field_data->maxlength) && isset($field_data->maxvalue)) {

            $qry .= " WHEN  " . $maxlength_cond . " AND " . $maxvalue_cond . " AND  " . $errorlog_isnull_cond . "  THEN  '" . $error_length . $error_value . "'
                      WHEN  " . $maxlength_cond . " AND " . $errorlog_isnull_cond . "  THEN  '" . $error_length . "'
                      WHEN  " . $maxvalue_cond . " AND " . $errorlog_isnull_cond . " THEN '" . $error_value . "
                      END ) ";

            $qry2 = ", aj_isvalid = CASE WHEN length(" . $field_name . ") > " . $field_data->maxlength . " THEN  'N' ELSE ''   END ";

        } else if (isset($field_data->maxlength)) {

            $qry .= " WHEN  " . $maxlength_cond . " AND " . $errorlog_isnull_cond . "  THEN  '" . $error_length . "'
                                    ELSE  CONCAT(aj_error_log,'" . $error_length . "')

                      END ) ";

            $qry2 = ", aj_isvalid = CASE WHEN length(" . $field_name . ") > " . $field_data->maxlength . " THEN  'N'  ELSE ''   END ";

        }

        if (isset($field_data->maxvalue)) {

            $qry .= " WHEN  " . $maxvalue_cond . " AND " . $errorlog_isnull_cond . " THEN '" . $error_length . "'
                                ELSE  CONCAT(aj_error_log,'" . $error_value . "')
                    END ) ";
            $qry2 = ", aj_isvalid = CASE WHEN length(" . $field_name . ") > " . $field_data->maxlength . " THEN  'N' ELSE ''  END ";
        }

        $qry .= $qry2;

        $qry .= " WHERE aj_isvalid!='N'  AND id in (SELECT id FROM(SELECT id FROM " . $temp_table_name . " ttb2  ORDER BY ttb2.id ASC LIMIT " . $limit . ", " . $batchsize . ")  ttb2 )";

        //echo "<br/>" . $qry;

        try {

            Log::info('<br/> \n update query  :----------------------------------');
            Log::info($qry);

            $update_res = DB::update($qry);

            /*Log::info($update_res);
            Log::info("==========================================================================================");*/
            unset($update_res);

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.

            $this->errors[] = $ex->getMessage();

            /*Log::info($ex->getMessage());
        Log::info($ex);
        Log::info("*********************************************************************");*/
        }
        unset($qry);
        unset($$qry2);

    }

    public function validateFieldType($field_name, $field_data)
    {
        $temp_table_name = $this->table_name;

        $qry = "";

        if ($field_data->FieldType == "INT") {
            $error['int'] = "Warning: #1366 Incorrect integer value: for column '" . $field_name . "' ";
            $qry          = "UPDATE " . $temp_table_name . " SET aj_error_log = '" . $error['int'] . "'  WHERE " . $field_name . " NOT REGEXP '^[0-9]+$'";
        } else if ($field_data->FieldType == "DATETIME") {
            $error['datetime'] = "Warning: #1265 Data truncated for column '" . $field_name . "'";
            $qry               = "UPDATE " . $temp_table_name . " SET aj_error_log = '" . $error['datetime'] . "'  WHERE TO_DAYS(STR_TO_DATE(" . $field_name . ", '%d-%b-%Y')) IS NULL  ";
        }

        if ($qry != "") {
            try {
                DB::update($qry);

            } catch (\Illuminate\Database\QueryException $ex) {

                // Note any method of class PDOException can be called on $ex.

                $this->errors[] = $ex->getMessage();
            }

        }

    }

    /**
     * Validate temp table for the fields having unique  values
     * If multiple records exists with the same field value in temp table, will be marked as invalid row
     *
     * @param      string  $field_name  The field name
     */
    public function validatePrimaryUnique($field_name)
    {

        $temp_table_name  = $this->table_name;
        $uniq_field_error = " Multiple Values for the unique field in the data provided";

        $qry_validate_uniq = "UPDATE " . $temp_table_name . " SET aj_error_log='" . $uniq_field_error . "', aj_isvalid = 'N' WHERE " . $field_name . " IN (SELECT " . $field_name . " FROM (SELECT tt1." . $field_name . " as " . $field_name . "  FROM " . $temp_table_name . " tt1  group by " . $field_name . " having count(" . $field_name . ")>1) tt2)";

        Log::info("-------------ValidatePrimaryUnique----------------");
        Log::info($qry_validate_uniq);

        try {

            DB::update($qry_validate_uniq);

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.

            $this->errors[] = $ex->getMessage();

            /*Log::info($ex->getMessage());
        Log::info($ex);
        Log::info("*********************************************************************");*/
        }
        unset($qry_validate_uniq);
    }

}
