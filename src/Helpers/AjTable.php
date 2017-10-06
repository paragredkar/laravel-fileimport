<?php
namespace Ajency\Ajfileimport\Helpers;

use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;

/**
 * Class to validate the csv data imported to temporary table, based on master/child table schema
 */
class AjTable
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

    public function getFormatedTableHeaderName($header)
    {

        return str_replace(' ', '_', $header);
    }

    public function __construct($table_name)
    {
        $this->table_name = $table_name;
    }

    public function doesTableExist()
    {

        $qry_table_exists = "SHOW TABLES LIKE '" . $this->table_name . "'";

        try {

            $table_exists = DB::select($qry_table_exists);

            /* echo "<br/> Table exists";
            print_r($table_exists);*/

            if (is_array($table_exists) && count($table_exists) > 0) {
                $this->exists = true;
            } else {
                $this->errors[] = "Table '" . $this->table_name . "' does not exist.";
            }

            return $this->exists;

        } catch (\Illuminate\Database\QueryException $ex) {
            // Note any method of class PDOException can be called on $ex.
            $this->errors[] = $ex->getMessage();

            return $this->exists;

        }

    }

    public function getTableSchema()
    {
        return $this->fields;
    }

    public function setTableSchema()
    {
        $table_exists = $this->doesTableExist();

        if ($table_exists == false || count($this->errors) > 0) {

            echo "<br>Cannot proceed with import. Please correct the following errors first:<br>";
            $eror_cnt = 1;
            foreach ($this->errors as $key => $value) {
                echo "<br>" . $eror_cnt . ". " . $value;
            }

            return false;
        }

        $qry_table_schema = "EXPLAIN " . $this->table_name;

        try {

            $this->fields = DB::select($qry_table_schema);

            /* $pdo_obj = DB::connection()->getpdo();
            $result  = $pdo_obj->exec($qry_table_schema); */

            foreach ($this->fields as $key => $value) {

                $field_key              = $this->getFormatedTableHeaderName($value->Field);
                $new_fields[$field_key] = $this->setFieldSizeByTablestructure($value);

            }

            $this->fields = $new_fields;

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.

            $this->errors[] = $ex->getMessage();
        }
    }

    /**
     *
     *
     * @param      <type>  $field  The field
     *
     * @return     <type>  The field length.
     */
    public function setFieldSizeByTablestructure($field)
    {
        $field_type_up = strtoupper($field->Type);

        //echo "<br/>---- " . $field_type_up;

        if (strpos($field_type_up, 'TINYINT') !== false) {
            $field->minval           = -128;
            $field->maxval           = 127;
            $field->FieldType        = 'TINYINT';
            $field->tmp_field_type   = 'VARCHAR';
            $field->tmp_field_length = 50;

        } else if (strpos($field_type_up, 'SMALLINT') !== false) {

            $field->minval           = -32768;
            $field->maxval           = 32767;
            $field->FieldType        = 'SMALLINT';
            $field->tmp_field_type   = 'VARCHAR';
            $field->tmp_field_length = 50;

        } else if (strpos($field_type_up, 'MEDIUMINT') !== false) {
            $field->minval           = -8388608;
            $field->maxval           = 8388607;
            $field->FieldType        = 'MEDIUMINT';
            $field->tmp_field_type   = 'VARCHAR';
            $field->tmp_field_length = 50;

        } else if (strpos($field_type_up, 'INT') !== false) {
            $field->minval           = -2147483648;
            $field->maxval           = 2147483647;
            $field->FieldType        = 'INT';
            $field->tmp_field_type   = 'VARCHAR';
            $field->tmp_field_length = 50;

        } else if (strpos($field_type_up, 'BIGINT') !== false) {
            $field->minval           = -9223372036854775808;
            $field->maxval           = 9223372036854775807;
            $field->FieldType        = 'BIGINT';
            $field->tmp_field_type   = 'VARCHAR';
            $field->tmp_field_length = 50;

        }
        /* Float/double/decimal */

        else if (strpos($field_type_up, 'VARCHAR') !== false || strpos($field_type_up, 'CHAR') !== false) {
            $field_explode1        = explode("(", $field_type_up);
            $field_explode2        = explode(")", $field_explode1[1]);
            $field->maxlength      = $field_explode2[0];
            $field->fieldmaxlength = $field_explode2[0];

            if (strpos($field_type_up, 'VARCHAR') !== false) {
                $fieldtype = "VARCHAR";

                $field->tmp_field_type   = 'VARCHAR';
                $field->tmp_field_length = $field->maxlength + 50;

            } else if (strpos($field_type_up, 'CHAR') !== false) {
                $fieldtype = "CHAR";

                $field->tmp_field_type = 'VARCHAR';

                $field->tmp_field_length = (isset($field->maxlength) ? $field->maxlength : 0) + 50;
            }

            $field->FieldType = $fieldtype;
        } else if (strpos($field_type_up, 'TINYTEXT') !== false) {
            $field->maxlength      = 255;
            $field->FieldType      = 'TINYTEXT';
            $field->tmp_field_type = 'TEXT';

        } else if (strpos($field_type_up, 'TEXT') !== false) {
            $field->maxlength      = 65535;
            $field->FieldType      = 'TEXT';
            $field->tmp_field_type = 'LONGTEXT';

        } else if (strpos($field_type_up, 'LONGTEXT ') !== false) {
            $field->maxlength = 4294967295;
            $field->FieldType = 'LONGTEXT';

        } else {
            $field->FieldType        = $field_type_up;
            $field->tmp_field_type   = 'VARCHAR';
            $field->tmp_field_length = 250;
        }

        return $field;

    }

    public function getConfigTableData()
    {

    }

    public function loadDataInChildTable()
    {
        $childtables = config('ajimportdata.childtables'); //Get child table from config
    }



     public function getUniqFields(){
            
            
            $table_schema = $this->getTableSchema();
            if(count($table_schema)<=0){
                $this->setTableSchema();
            }

            foreach ($table_schema as $child_field_name => $child_field_value) {
                if(isset($child_field_value->Key)){

                    

                    if($child_field_value->Key=="PRI" || $child_field_value->Key=="UNI"){

                       /* $child_field_maps = $child_table_conf_list[$child_count]['name'];
                        $child_field_maps_flipped = array_flip($child_field_maps);
                        if(isset($child_field_maps_flipped[$child_field_name])){

                            $temp_table_validator = new AjSchemaValidator($temp_tablename);
                            $temp_table_validator->validatePrimaryUnique($child_field_maps_flipped[$child_field_name]);

                        }*/

                        $uniq_fields[] = $child_field_value->Field;
                    }
                }
            }
            /* End validating temp table for uniq field values*/

            return $uniq_fields;
    }

}
