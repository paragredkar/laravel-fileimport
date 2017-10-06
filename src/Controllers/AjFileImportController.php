<?php
/**
 * Ajency Laravel CSV Import Package
 */

namespace Ajency\Ajfileimport\Controllers;

use Ajency\Ajfileimport\Helpers\AjCsvFileImport;
use Ajency\Ajfileimport\Helpers\AjTable;
/*use App\Jobs\AjImportDataJob;*/
use App\Http\Controllers\Controller;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
//use Ajency\Ajfileimport\jobs\AjImportDataJob;
use Illuminate\Support\Facades\Storage; //test
use Log;
//test

class AjFileImportController extends Controller
{

    /**
     * { item_description }
     * Type : Test
     */
    public function isDirExists()
    {

        $allfiles = Storage::allFiles('app/Ajency/Ajfileimport/Files/');

        print_r($allfiles);

        $exists = Storage::disk('local')->has('asd');

        $result = Storage::disk('local')->exists('asd');

        echo "<pre> is dir exists";
        print_r($result);
        print_r($exists);

        echo storage_path();

        $import_dir = storage_path('app/Ajency/');
        echo $import_dir;
        $directories = Storage::allDirectories('app');

        print_r($directories);
    }

    public function testTableStructure()
    {
        $table = new AjTable('testuser');
        $table->setTableSchema();
        echo "<pre>";
        print_r($table->getTableSchema());

    } 

    /**
     * Reads a file.
     */
    public function showUploadFile()
    {

        return view('ajfileimport::index');

    }

    public function uploadFile(Request $request)
    {
        $aj_file_import = new AjCsvFileImport();

        $aj_file_import->init($request);

    }

    public function getErrorLogs(){
        $aj_file_import = new AjCsvFileImport();

        $result = $aj_file_import->sendErrorLogFile();
    }

    public function testSchedule(){
        $aj_file_import = new AjCsvFileImport();

        $result = $aj_file_import->testSchedule();   
    }

 

}
