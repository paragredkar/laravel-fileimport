<?php
/**
 * Ajency Laravel CSV Import Package
 */
namespace Ajency\Ajfileimport\Helpers;

use Ajency\Ajfileimport\Helpers\AjSchemaValidator;
use Ajency\Ajfileimport\Helpers\AjTable;
use Ajency\Ajfileimport\jobs\AjImportDataJob;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Database\QueryException;

//Added to schedule the job queue
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\DB; //files storage for import

//use Illuminate\Filesystem\Filesystem;

use Illuminate\Support\Facades\File;
//Added to schedule the job queue
use Log;

/*use View;
use Ajency\Ajfileimport\Views\AjSchemaValidator;*/

/**
 * Class for aj csv file import.
 */
class AjCsvFileImport
{

    private $temp_table_headers;
    private $file_path;
    private $errors;

    public function __construct($file_path = "")
    {
        if ($file_path != "") {
            $this->file_path = $file_path;
        }

    }

    public function printErrorLog($error_logs)
    {

        foreach ($error_logs as $key => $error) {
            echo $key . ". " . $error;
        }

    }

    public function init($request)
    {

        $prev_pending_jobs = $this->areTherePreviousJobsPending();

        if ($prev_pending_jobs === true) {
            $this->printErrorLog($this->getErrorLogs());
            exit();
        }

        $res = $this->clearPreviousImportFiles();

        $file_handle = new FileHandler();
        $result      = $file_handle->storeFile($request);
        if ($result == false) {
            return;
        }

        $file_path = $file_handle->getFilePath();

        $this->setFilePath($file_path);
        $this->importFileData($file_handle);

    }

    public function getErrorLogs()
    {
        return $this->errors;
    }

    public function areTherePreviousJobsPending()
    {

        try {
            $res_pending_job_count = DB::select("SELECT count(*) as pending_job_count FROM jobs WHERE queue in ('validateunique','validatechildinsert','insertvalidchilddata','tempupdatechildid','masterinsert')");

            $pending_job_count = $res_pending_job_count[0]->pending_job_count;

            if ($pending_job_count > 0) {
                $this->errors[] = "There are pending jobs from previous import to be processed!!";
                return true;
            } else {

                return false;
            }

        } catch (\Illuminate\Database\QueryException $ex) {

            $this->errors[] = $ex->getMessage();
        }
    }

    public function clearPreviousImportFiles()
    {

        $folder_to_clean[] = storage_path('app/Ajency/Ajfileimport/validchilddata');
        $folder_to_clean[] = storage_path('app/Ajency/Ajfileimport/mtable');
        $folder_to_clean[] = storage_path('app/Ajency/Ajfileimport/Files');

        foreach ($folder_to_clean as $folder) {
            $result = File::cleanDirectory($folder);
        }
    }

    public function setFilePath($file_path)
    {
        $this->file_path = $file_path;
    }

    public function getFilePath()
    {
        return $this->file_path;
    }

    public function index()
    {

        return view('ajency/ajfileimport::index');

    }

    public function uploadFile()
    {

    }

    /**
     * Loads a filedata in table.
     */
    public function loadFiledataInTable($temp_table_headers)
    {
        foreach ($temp_table_headers as $header) {
            $this->$temp_table_headers[] = str_replace(' ', '_', $header);
        }

    }

    public function getFormatedTableHeaderName($header)
    {

        return str_replace(' ', '_', $header);
    }

    public function getFormatedFieldName($field_name)
    {

        return str_replace(' ', '_', $field_name);
    }

    public function importFileData($file_handle)
    {

        $this->loadFileData($file_handle);
        $temp_tablename     = config('ajimportdata.temptablename');
        $res_table_creation = $this->createTempTable();

        if ($res_table_creation['success'] == false) {
            foreach ($res_table_creation['errors'] as $key => $error) {
                echo "<br/>" . $error;
            }
            return;
        }

        $real_file_path = $this->getFilePath();

        $file_headers = $file_handle->getFileHeaders();
        $this->loadFiledatainTempTable($real_file_path, $file_headers, $temp_tablename);
        //$this->insertUpdateChildTable(); //VALIDATING CHILD TABLE FIELDS
    }

    public function loadFileData($file)
    {
        DB::connection()->disableQueryLog();

        /* $real_file_path = $this->getFilePath();
        $file           = new FileHandler(array('filepath' => $real_file_path));*/

        $result = $file->isValidFile();

        if ($result['success'] !== true) {

            if (count($result['error']) >= 0) {

                $file_error = $result['error'];
                foreach ($file_error as $err_key => $err_value) {
                    echo "<br/>" . $err_value;
                }
            } else {
                echo "Invalid File.";
            }
            return false;

        }
        /* echo "<pre>";
    print_r($result);*/

    }

    /**
     * Match with child/master table field and get the temp table field in query
     * temp table field type and sizes are set on mastertable/child table class, when setTableschema is called on the ajtable class
     * @param      <type>   $mastertable_conf  The mastertable conf
     * @param      <type>   $mastertable       The mastertable
     * @param      boolean  $is_child          Indicates if child
     *
     * @return     string   ( description_of_the_return_value )
     */
    public function tempTableQueryByTable($mastertable_conf, $mastertable, $is_child)
    {

        $mtable_fieldmaps = $mastertable_conf['fields_map'];

        $qry__create_table = "";

        $mtable_fields = $mastertable->getTableSchema();

        $mtable_flipped_fieldmaps = array_flip($mastertable_conf['fields_map']);

        foreach ($mtable_flipped_fieldmaps as $mfield_key => $mfield_value) {

            $mfield_key = $mastertable->getFormatedTableHeaderName($mfield_key);

            $tfield_name = $this->getFormatedTableHeaderName($mfield_value);

            $cur_mtable_field = $mtable_fields[$mfield_key];

            //$qry__create_table .= "<br/>";

            $qry__create_table .= ", ";
            $qry__create_table .= $tfield_name . " " . $cur_mtable_field->tmp_field_type;
            if (isset($cur_mtable_field->tmp_field_length)) {

                $qry__create_table .= "(" . $cur_mtable_field->tmp_field_length . ")";
            }

            if ($cur_mtable_field->Null == true) {

                $qry__create_table .= " DEFAULT NULL ";
            }

            if (isset($cur_mtable_field->Default)) {

                $qry__create_table .= " DEFAULT " . $cur_mtable_field->Default;
            }

            //if child table create index on the field
            if ($is_child == true) {
                $qry__create_table .= ", INDEX " . $tfield_name . " (" . $tfield_name . " )";
            }

            //$mfield_data = $mastertable_fields[$mfield_key];
            //print_r($mtable_fields[$mfield_key]);

        }

        return $qry__create_table;
    }

    public function createTempTable()
    {

        $fileheaders_conf = config('ajimportdata.fileheader'); //Get file headers
        //
        $mastertable_conf = config('ajimportdata.mastertable'); //Get file headers

        $childtables_conf = config('ajimportdata.childtables'); //Get child table from config

        $temp_table_name = config('ajimportdata.temptablename'); //Get temp table name from config

        $this->deleteTable($temp_table_name);

        $qry__create_table = "CREATE TABLE IF NOT EXISTS " . $temp_table_name . " (
                                    `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY";

        $qry_childtable_insert_ids = "";

        $mastertable = new AjTable($mastertable_conf['name']);
        $mastertable->setTableSchema();
        $is_child_table = false;
        $qry__create_table .= $this->tempTableQueryByTable($mastertable_conf, $mastertable, $is_child_table);
        $qry_indexes = "";

        $child_count = 0;
        foreach ($childtables_conf as $child_data) {
            $is_child_table           = true;
            $childtable[$child_count] = new AjTable($child_data['name']);
            $childtable[$child_count]->setTableSchema();
            $qry__create_table .= $this->tempTableQueryByTable($child_data, $childtable[$child_count], $is_child_table);

            //if (isset($child_data['insertid_temptable'])) {
            if (isset($child_data['insertid_mtable'])) {

                $temptablefield_for_child_insertid = $this->getFormatedFieldName($child_data['name']) . "_id";

                $qry_childtable_insert_ids .= " ," . $temptablefield_for_child_insertid . " INT ";
                $qry_indexes .= ", INDEX USING BTREE(" . $temptablefield_for_child_insertid . ")";
            }

        }

        $qry__create_table .= $qry_childtable_insert_ids;

        $qry__create_table .= ", aj_error_log  LONGTEXT   ";
        $qry__create_table .= ", aj_isvalid  CHAR(1) NOT NULL DEFAULT '' ";
        $qry__create_table .= $qry_indexes;
        $qry__create_table .= " )  ENGINE=InnoDB;";

        Log::info("<pre>" . $qry__create_table);
        $success = false;
        $message = "";

        try {
            Log::info("<br/><br/>Creating 'Temp table' .....");
            //echo $qry__create_table;
            //echo qry__create_table;
            $create_table_result = DB::statement($qry__create_table);
            Log::info(($create_table_result));
            if ($create_table_result === true) {
                $message = "Temp table' created successfully!!";
                $success = true;
            }

        } catch (\PDOException $ex) {

            // Note any method of class PDOException can be called on $ex.

            $this->errors[] = $ex->getMessage();

            Log::info($this->errors);
            $success = false;
        }

        return array("success" => $success, 'errors' => $this->errors, 'message' => $message);

    }

    public function loadFiledatainTempTable($real_file_path, $file_headers, $temp_tablename)
    {

        $file_path = str_replace("\\", "\\\\", $real_file_path);

        $qry_load_data = "LOAD DATA LOCAL INFILE '" . $file_path . "' INTO TABLE " . $temp_tablename . "
                 FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '\"'
                LINES  TERMINATED BY '\n' IGNORE 1 LINES  ( ";
        $qry_load_data .= implode(",", $file_headers) . " ) ;    ";

        //echo $qry_load_data;

        try {

            $pdo_obj = DB::connection()->getpdo();
            $result  = $pdo_obj->exec($qry_load_data);
            /*  $pdo_warnings = $pdo_obj->exec('SHOW WARNINGS');*/

            //  var_dump($pdo_obj->events);

            // $pdo_warnings = $pdo_obj->exec('SHOW WARNINGS');
            Log::info($qry_load_data);
            Log::info($result);

            $this->validateTempTableFields();

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            echo "========================================== EXCEPTION <br/><br/>Row :" . $row_cnt . "<br/>";

            var_dump($ex->getMessage());
        }

        var_dump($result);

        /*if(($handle = fopen($file_path, 'r')) !== false)
    {
    // get the first row, which contains the column-titles (if necessary)
    $header = fgetcsv($handle);

    // loop through the file line-by-line
    $row_cnt = 0;
    while(($data = fgetcsv($handle)) !== false)
    {

    echo "<br/><br/>Row :".$row_cnt."<br/>";
    print_r($data);
    $row_cnt++;
    unset($data);
    }
    fclose($handle);
    } */

    }

    public function insertUpdateChildTable()
    {

        /*$childtable = new AjTable('testuser');
        $childtable->setTableSchema();
        $childvalidator = new AjSchemaValidator('testuser');
        $params = array('maxlength'=>50);
        $childvalidator->validateFieldLength('uemail',$params);*/

        $temp_tablename = config('ajimportdata.temptablename');
        $mtable         = new AjTable($temp_tablename);
        $mtable->setTableSchema();
        $mtablevalidator = new AjSchemaValidator($temp_tablename);
        $params          = array('maxlength' => 50);
        $mtablevalidator->validateFieldLength('email', $params);
    }

    public function addJobQueue()
    {

        $temp_tablename = config('ajimportdata.temptablename');

        $batchsize = config('ajimportdata.batchsize');

        //Get total valid record count from temp table and calculate batches
        try {

            $valid_record_count = DB::SELECT("SELECT COUNT(*) as records_count FROM " . $temp_tablename . " WHERE aj_isvalid!='N' ");

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            $this->errors[] = $ex->getMessage();

        }

        $temp_records_count = $valid_record_count[0]->records_count;

        $mastertable_conf = config('ajimportdata.mastertable');
        $mtable_name      = $mastertable_conf['name'];
        $mtable_fieldmaps = $mastertable_conf['fields_map'];

        $childtables_conf_ar = config('ajimportdata.childtables');

        $total_loops = round($temp_records_count / $batchsize);

        $this->addValidateUnique($temp_records_count);

        for ($loop = 0; $loop < $total_loops; $loop++) {

            $job_params = array('current_loop_count' => $loop, 'total_loops' => $total_loops, 'type' => 'insert_records');
            AjImportDataJob::dispatch($job_params)->onQueue('insert_records');

        }

        echo "<b>Note: Please run this command to complete the import of data: <br/> 'php artisan queue:work --queue=validateunique,insert_records'  </b>";
        Log::info("Executing schedule command");
        $app          = App::getFacadeRoot();
        $schedule     = $app->make(Schedule::class);
        $schedule_res = $schedule->command('php artisan queue:work --queue=validateunique,insert_records');
        echo "<pre>";
        print_r($schedule_res);

    }

    /**
     * Adds a validate unique.
     *
     * @param      integer  $temp_records_count  The temporary records count
     */
    public function addValidateUnique($temp_records_count)
    {

        Log::info('-------------addValidateUnique--------------');

        $temp_tablename        = config('ajimportdata.temptablename');
        $child_table_conf_list = config('ajimportdata.childtables');
        $total_no_child_tables = count($child_table_conf_list);

        $batchsize = config('ajimportdata.batchsize'); //Get temp table name from config
        $loops     = round($temp_records_count / $batchsize);

        for ($child_count = 0; $child_count < $total_no_child_tables; $child_count++) {

            $child_table = new AjTable($child_table_conf_list[$child_count]['name']);

            $child_table_schema = $child_table->setTableSchema();

            Log::info('<br/> \n  UNIQ keys for the table ');
            //add batch jobs to add uniq field validation on temp table
            $child_table_unique_keys = $child_table->getUniqFields();

            Log::info($child_table_unique_keys);

            $child_table_field_map      = $child_table_conf_list[$child_count]['fields_map'];
            $child_table_field_map_flip = array_flip($child_table_field_map);

            foreach ($child_table_unique_keys as $child_field_name) {

                if (isset($child_table_field_map_flip[$child_field_name])) {
                    $job_params = array('childtable' => $child_table_conf_list[$child_count], 'type' => 'validateunique', 'child_field_name' => $child_table_field_map_flip[$child_field_name]);
                    AjImportDataJob::dispatch($job_params)->onQueue('validateunique');
                }

            }

        }

    }

    public function addInsertRecordsQueue($params)
    {

        Log::info('-------------addInsertRecordsQueue--------------');

        $temp_tablename        = config('ajimportdata.temptablename');
        $child_table_conf_list = config('ajimportdata.childtables');
        $total_no_child_tables = count($child_table_conf_list);

        $batchsize = config('ajimportdata.batchsize'); //Get temp table name from config
        // $loops     = round($temp_records_count / $batchsize);

        for ($child_count = 0; $child_count < $total_no_child_tables; $child_count++) {

            $child_table = new AjTable($child_table_conf_list[$child_count]['name']);

            $child_table_schema = $child_table->setTableSchema();

            Log::info('<br/> \n  UNIQ keys for the table ');
            //add batch jobs to add uniq field validation on temp table
            $child_table_unique_keys = $child_table->getUniqFields();

            Log::info($child_table_unique_keys);

            //Add batch jobs on field validation for set batch of jobs

            $job_params = array('childtable' => $child_table_conf_list[$child_count], 'total_childs' => $total_no_child_tables, 'current_child_count' => $child_count);
            $job_params = array_merge($job_params, $params);
            //AjImportDataJob::dispatch($job_params)->onQueue('validatechildinsert');
            $this->processTempTableFieldValidation($job_params);

            /* $validchildinsert_params = array('childtable' => $child_table_conf_list[$child_count], 'loop_count' => $i, 'type' => 'insertvalidchilddata'  );
            AjImportDataJob::dispatch($validchildinsert_params)->onQueue('insertvalidchilddata');*/

            //$this->dispatch(new AjImportDataJob($job_params));
            /*Log::info("CURRENT CHILD COUNT :" . $child_count . " TOTAL CHILD COUNT :" . ($total_no_child_tables - 1));
            Log::info("CURRENT LOOP COUNT :" . $i . " tot  loops-1:" . ($loops - 1));
            if (($child_count == ($total_no_child_tables - 1)) && ($i == ($loops - 1))) {

            Log::info("Executing schedule command");
            $app      = App::getFacadeRoot();
            $schedule = $app->make(Schedule::class);
            $schedule_res = $schedule->exec('php artisan queue:work --queue=validateunique,validatechildinsert,insertvalidchilddata,tempupdatechildid,masterinsert');

            var_dump($schedule_res );*/
            /*  //Run job queue
        Artisan::call('queue:work', [
        '--queue' => 'validateunique,childinsert,tempupdatechildid,masterinsert'
        ]);*/
        }

    }

    /* #######################################################################################################################################     */

    public function validateTempTableFields()
    {

        Log::info('----------validateTempTableFields---- beffore try block------');

        $temp_tablename = config('ajimportdata.temptablename');

        try {

            $temp_records_count_res = DB::SELECT("SELECT COUNT(*) as records_count FROM " . $temp_tablename);
            Log::info('----------validateTempTableFields----------');
            $temp_records_count = $temp_records_count_res[0]->records_count;
            Log::info($temp_records_count);
            // $this->generateJobQueue($temp_records_count);
            $this->addJobQueue();

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            $this->errors[] = $ex->getMessage();

        }

    }

    public function processUniqueFieldValidationQueue($params)
    {

        $temp_tablename   = config('ajimportdata.temptablename');
        $child_field_name = $params['child_field_name'];

        $temp_table_validator = new AjSchemaValidator($temp_tablename);
        $temp_table_validator->validatePrimaryUnique($child_field_name);

    }

    public function processTempTableFieldValidation($params)
    {

        $temp_tablename   = config('ajimportdata.temptablename');
        $child_table_conf = $params['childtable'];
        $loop_count       = $params['current_loop_count'];

        $child_table = new AjTable($child_table_conf['name']);

        $child_table->setTableSchema();

        $child_table_schema = $child_table->getTableSchema();

        // echo "<pre>";
        // print_r($child_table->getTableSchema());

        $child_field_maps = $child_table_conf['fields_map'];

        //print_r($child_field_maps);

        $temp_table_validator = new AjSchemaValidator($temp_tablename);

        foreach ($child_field_maps as $temp_field_name => $child_field_name) {

            $temp_table_validator->validateField($temp_field_name, $child_table_schema[$child_field_name], $loop_count);

        }
        $this->exportValidTemptableDataToFile($params);

        /* $job_params = array('childtable' => $child_table_conf, 'loop_count' => $loop_count, 'type' => 'insertvalidchilddata');
        AjImportDataJob::dispatch($job_params)->onQueue('insertvalidchilddata');*/

        // $this->exportValidTemptableDataToFile($child_table_conf, $temp_tablename, $loop_count);

    }

    //public function exportValidTemptableDataToFile($child_table_conf, $temp_tablename, $loop_count)
    public function exportValidTemptableDataToFile($params)
    {

        $child_table_conf = $params['childtable'];

        $total_childs        = $params['total_childs'];
        $total_batches       = $params['total_loops'];
        $current_child_count = $params['current_child_count'];

        $loop_count = $params['current_loop_count'];

        $temp_tablename = config('ajimportdata.temptablename');

        $child_field_maps = $child_table_conf['fields_map'];

        $child_table_name = $child_table_conf['name'];

        $temp_fields_ar = array_keys($child_field_maps);
        $temp_fields    = implode(",", $temp_fields_ar);

        $child_fields_ar = array_values($child_field_maps);
        $child_fields    = implode(",", $child_fields_ar);

        $batchsize = config('ajimportdata.batchsize');

        $limit = $loop_count * $batchsize;

        $file_prefix = "aj_" . $child_table_name;
        $folder      = storage_path('app/Ajency/Ajfileimport/validchilddata/');

        $this->createDirectoryIfDontExists($folder);

        $child_outfile_name = $this->generateUniqueOutfileName($file_prefix, $folder);

        //$child_outfile_name = "aj_" . $child_table_name . "" . date('d_m_Y_H_i_s') . ".csv";

        //$child_outfile_name = storage_path('app/Ajency/Ajfileimport/validchilddata/') . $child_outfile_name;

        // $request->file('ajfile')->storeAs('Ajency/Ajfileimport/Files', $new_file_name);

        $file_path = str_replace("\\", "\\\\", $child_outfile_name);

        try {

            $qry_select_valid_data = "SELECT " . $temp_fields . " INTO OUTFILE '" . $file_path . "'
                                    FIELDS TERMINATED BY ','
                                    OPTIONALLY ENCLOSED BY '\"'
                                    LINES TERMINATED BY '\n'
                                    FROM " . $temp_tablename . " outtable WHERE outtable.id in (SELECT id FROM (SELECT id FROM " . $temp_tablename . " tt   ORDER BY tt.id ASC LIMIT " . $limit . "," . $batchsize . ") tt2 )  AND  aj_isvalid!='N'";

            /*Log::info('<br/> \n  OUTFILE query  :----------------------------------');
            Log::info("filepath". $file_path);
            Log::info( $qry_select_valid_data);*/

            DB::select($qry_select_valid_data);

            //update valid rows in temp table with the valid inserts on child table.

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            $this->errors[] = $ex->getMessage();

        }

        //Load valid data from temp table into child table

        $qry_load_data = "LOAD DATA LOCAL INFILE '" . $file_path . "' INTO TABLE " . $child_table_name . "
         FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '\"'
        LINES  TERMINATED BY '\n'    ( ";
        $qry_load_data .= $child_fields . " ) ";

        try {
            $pdo_obj = DB::connection()->getpdo();
            $result  = $pdo_obj->exec($qry_load_data);

            Log::info($qry_load_data);

            $job_params_update_child_id = array('childtable' => $child_table_conf, 'current_loop_count' => $loop_count, 'type' => 'tempupdatechildid', 'total_childs' => $total_childs, 'total_loops' => $total_batches, 'current_child_count' => $current_child_count);

            $this->UpdateTempTableWithChildInsertIds($job_params_update_child_id);
            //AjImportDataJob::dispatch($job_params_update_child_id)->onQueue('tempupdatechildid');

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            Log::info($ex->getMessage());
            Log::info($ex);

            Log::info("**********************************ERROR************************************");

        }

        //Update temp table with insert ids of child table

    }

    public function UpdateTempTableWithChildInsertIds($params)
    {

        $temp_tablename      = config('ajimportdata.temptablename');
        $child_table_conf    = $params['childtable'];
        $loop_count          = $params['current_loop_count'];
        $total_childs        = $params['total_childs'];
        $total_batches       = $params['total_loops'];
        $current_child_count = $params['current_child_count'];

        $child_insert_id_on_temp_table = $this->getFormatedFieldName($child_table_conf['name']) . "_id"; // $child_table_conf['insertid_temptable'];
        $child_insert_id_field         = $child_table_conf['insertid_childtable'];

        $batchsize = config('ajimportdata.batchsize');

        $limit = $loop_count * $batchsize;

        $field_maps      = $child_table_conf['fields_map'];
        $cnt_where       = 0;
        $where_condition = " ";
        foreach ($field_maps as $tempfield => $childfield) {

            $where_condition .= " AND ";

            $where_condition .= " tmpdata." . $tempfield . "=" . "childtable." . $childfield . "";
            $cnt_where++;
        }

        $qry_update_child_ids = "UPDATE " . $temp_tablename . " tmpdata, " . $child_table_conf['name'] . " childtable
        SET
            tmpdata." . $child_insert_id_on_temp_table . " = childtable." . $child_insert_id_field . "
        WHERE  tmpdata.id in (SELECT id FROM (SELECT id FROM " . $temp_tablename . " tt ORDER BY tt.id ASC LIMIT " . $limit . "," . $batchsize . ") tt2 )  AND  tmpdata.aj_isvalid!='N'" . $where_condition;

        try {

            Log::info('<br/> \n  UPDATER child ids on temp table   :----------------------------------');

            Log::info($qry_update_child_ids);

            DB::update($qry_update_child_ids);

            //update valid rows in temp table with the valid inserts on child table.

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            $this->errors[] = $ex->getMessage();

        }

        $string = "Total child count : " . ($total_childs - 1) . " total batches :" . ($total_batches - 1);

        Log::info($qry_update_child_ids);
        Log::info($string);

        if ($current_child_count == ($total_childs - 1)) {

            Log::info('CALL MASTER INSERT NOW');
            $this->process_masterinsert_queue($params);
        } else {
            Log::info('DO NOT CALL MASTER INSERT NOW');

        }

    }

    public function process_masterinsert_queue($params)
    {

        $mastertable_conf = config('ajimportdata.mastertable');
        $mtable_name      = $mastertable_conf['name'];

        $mtable_fieldmaps = $mastertable_conf['fields_map'];

        $childtables_conf_ar = config('ajimportdata.childtables');

        foreach ($childtables_conf_ar as $key => $childtable_conf) {
            $child_fieldmaps                   = $childtable_conf['fields_map'];
            $temptablefield_for_child_insertid = $this->getFormatedFieldName($childtable_conf['name']) . "_id";

            //if (isset($childtable_conf['insertid_mtable']) && isset($childtable_conf['insertid_temptable'])) {
            if (isset($childtable_conf['insertid_mtable'])) {

                $mtable_fieldmaps[$temptablefield_for_child_insertid] = $childtable_conf['insertid_mtable'];

            }

        }

        $temp_tablename = config('ajimportdata.temptablename');

        $batchsize = config('ajimportdata.batchsize');

        $loop_count = $params['current_loop_count'];

        $total_loops = $params['total_loops'];

        $mtable_name = $mastertable_conf['name'];

        $limit = $loop_count * $batchsize;

        $master_outfile_name = "aj_" . $mtable_name . "" . date('d_m_Y_H_i_s') . ".csv";

        $file_prefix = "aj_" . $mtable_name;
        $folder      = storage_path('app/Ajency/Ajfileimport/mtable');

        $this->createDirectoryIfDontExists($folder);

        $master_outfile_path = $this->generateUniqueOutfileName($file_prefix, $folder);

        //$master_outfile_path = storage_path('app/Ajency/Ajfileimport/mtable/') . $master_outfile_name;

        $file_path = str_replace("\\", "\\\\", $master_outfile_path);

        $tempfield_ar = array_keys($mtable_fieldmaps);

        $temp_fields = implode(',', $tempfield_ar);

        try {

            $qry_select_valid_data = "SELECT " . $temp_fields . " INTO OUTFILE '" . $file_path . "'
                                    FIELDS TERMINATED BY ','
                                    OPTIONALLY ENCLOSED BY '\"'
                                    LINES TERMINATED BY '\n'
                                    FROM " . $temp_tablename . " outtable WHERE outtable.id in (SELECT id FROM (SELECT id FROM " . $temp_tablename . " tt LIMIT " . $limit . "," . $batchsize . ") tt2 )  AND  aj_isvalid!='N'";

            Log::info('<br/> \n  OUTFILE MASTER TABLE QUERY   :----------------------------------');
            Log::info("filepath" . $file_path);
            Log::info($qry_select_valid_data);

            DB::select($qry_select_valid_data);

            $this->loadMasterData($file_path, $mtable_fieldmaps, $mastertable_conf, $params);

            //update valid rows in temp table with the valid inserts on child table.

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            $this->errors[] = $ex->getMessage();

        }

    }

    public function loadMasterData($file_path, $mtable_fieldmaps, $mastertable_conf, $params)
    {

        $total_records = $params['total_loops'];
        $loop_count    = $params['current_loop_count'];

        $mtable_name = $mastertable_conf['name'];

        $mtable_fields_ar = array_values($mtable_fieldmaps);
        $mtable_fields    = implode(",", $mtable_fields_ar);

        $qry_mtableload_data = "LOAD DATA LOCAL INFILE '" . $file_path . "' INTO TABLE " . $mastertable_conf['name'] . "
                 FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '\"'
                LINES  TERMINATED BY '\n'    ( ";
        $qry_mtableload_data .= $mtable_fields . " ) ";

        Log::info('<br/> \n  LOAD IN  MASTER TABLE QUERY   :----------------------------------');
        Log::info("filepath" . $file_path);
        Log::info($qry_mtableload_data);

        try {
            $pdo_obj = DB::connection()->getpdo();
            $result  = $pdo_obj->exec($qry_mtableload_data);
            Log::info("---------- Load  data in master table ---------");
            Log::info($qry_mtableload_data);

        } catch (\Illuminate\Database\QueryException $ex) {

            // Note any method of class PDOException can be called on $ex.
            Log::info($ex->getMessage());
            Log::info($ex);

            Log::info("**********************************ERROR************************************");

        }

        Log::info('Loop count:' . $loop_count . " totalrecords - 1 " . ($total_records - 1));

        //If insertion of records to master table is done send mail of records with error to given email
        if ($loop_count == ($total_records - 1)) {
            $this->sendErrorLogFile();

        }

    }

    public function sendErrorLogFile()
    {

        $temp_tablename = config('ajimportdata.temptablename');
        $file_prefix    = "aj_errorlog";
        $folder         = storage_path('app/Ajency/Ajfileimport/errorlogs/');

        $errorlog_outfile_path = $this->generateUniqueOutfileName($file_prefix, $folder);

        echo $errorlog_outfile_path;

        $file_path = str_replace("\\", "\\\\", $errorlog_outfile_path);

        try {

            $qry_select_valid_data = "SELECT  * INTO OUTFILE '" . $file_path . "'
                                    FIELDS TERMINATED BY ','
                                    OPTIONALLY ENCLOSED BY '\"'
                                    LINES TERMINATED BY '\n'
                                    FROM " . $temp_tablename . " outtable WHERE aj_isvalid='N'";

            DB::select($qry_select_valid_data);

        } catch (\Illuminate\Database\QueryException $ex) {

            $this->errors[] = $ex->getMessage();

        }

    }

    public function deleteTable($table_name)
    {

        $qry_drop_table = "DROP TABLE IF EXISTS " . $table_name;

        try {
            $pdo_obj = DB::connection()->getpdo();
            $result  = $pdo_obj->exec($qry_drop_table);
            Log::info($qry_drop_table);

        } catch (\Illuminate\Database\QueryException $ex) {

            Log::info($ex->getMessage());
            $this->errors[] = $ex->getMessage();

        }

    }

    public function generateUniqueOutfileName($prefix, $folder)
    {

        $rand_string = $this->getRandomString(4);
        $file_path   = $folder . "/" . $prefix . "_" . $rand_string . "_" . date('d_m_Y_H_i_s') . ".csv";
        if (file_exists($file_path)) {
            $this->generateUniqueOutfileName($prefix, $folder);
        } else {
            return $file_path;
        }

    }

    /**
     * function to generate random strings
     * @param       int     $length     number of characters in the generated string
     * @return      string  a new string is created with random characters of the desired length
     */
    public function getRandomString($length = 4)
    {
        $randstr = "";
        // srand((double) microtime(TRUE) * 1000000);
        //our array add all letters and numbers if you wish
        $chars = array(
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'p',
            'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5',
            '6', '7', '8', '9');

        for ($rand = 0; $rand <= $length; $rand++) {
            $random = rand(0, count($chars) - 1);
            $randstr .= $chars[$random];
        }
        return $randstr;
    }

    public function testSchedule()
    {
        Log::info("Executing schedule command");
        $app          = App::getFacadeRoot();
        $schedule     = $app->make(Schedule::class);
        $schedule_res = $schedule->command('queue:work --queue=validateunique,insert_records');
        echo "<pre>";
        print_r($schedule_res);
    }

    public function is_directory_exists($filepath)
    {
        if (File::exists($filepath)) {
            if (File::isDirectory($filepath)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }

    }

    public function createDirectoryIfDontExists($filepath)
    {

        if (!$this->is_directory_exists($filepath)) {
            File::makeDirectory($filepath, 0775, true, true);
        }

    }

}
