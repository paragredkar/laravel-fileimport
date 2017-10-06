<?php

/**
 * Ajency Laravel CSV Import
 * File read and validations
 */

namespace Ajency\Ajfileimport\Helpers;

class FileHandler
{

    private $file_path;
    private $delimiter;
    private $file_type;
    private $header_count_match = true;
    private $header_matched     = true;
    private $errors             = [];
    private $logs               = [];
    private $headers            = [];

    public function __construct($param = array())
    {
        if (isset($param['filepath'])) {
            $this->file_path = $param['filepath'];
        }

    }

    public function storeFile($request)
    {

        if(is_null($request->file('ajfile'))){
            echo "Please select file.";
            return false;
        }    

        $temp_path       = $request->file('ajfile')->getRealPath();

        if(!file_exists($temp_path) ){
            echo "Please select file.";
            return false;
        }
        $new_file_name   = 'ajimportfile'.date('d_m_Y_H_i_s').'.csv';
        $this->file_path = storage_path('app/Ajency/Ajfileimport/Files/') . $new_file_name;

        $request->file('ajfile')->storeAs('Ajency/Ajfileimport/Files', $new_file_name);

        return $this->file_path;

    }

    public function getFilePath()
    {
        return $this->file_path;
    }

    public function getFileHeaders()
    {
        return $this->headers;
    }

    /**
     * Check if the file contains Headers,
     * Returns true if file header matches with the headers provided in configuratio file
     */
    public function isValidFile()
    {

        $file_type = config('ajimportdata.filetype');
        $error     = [];

        echo "<br/>Validating file....";
        //echo "File Type : "$file_type;
        switch ($file_type) {

            case 'csv':

                $this->normalizeLineEndings();

                $config_fileheaders       = config('ajimportdata.fileheader');
                $file_headers             = $this->read_csv_file_headers();
                $config_fileheaders_count = count($config_fileheaders);
                $file_headers_count       = count($file_headers);

                /*print_r($config_fileheaders);
                echo " config header count :". $config_fileheaders_count . "-- File Header count:" . $file_headers_count;*/
                if ($config_fileheaders_count != $file_headers_count) {
                    $error[]                  = "Error: Header count mismatched <br/> File Headers count: " . $file_headers_count . " <br/> Config file header count:" . $config_fileheaders_count;
                    $this->header_count_match = false;
                    break;
                } else {

                    $logs[] = "<br/> Header count matched ";
                }

                $header_count = count($config_fileheaders);
                for ($i = 0; $i < $header_count; $i++) {
                    if ($config_fileheaders[$i] != $file_headers[$i]) {
                        $error[]              = "Error: File Headers mismatched with the configuration!!";
                        $this->header_matched = false;
                        break;
                    }

                }
                $this->headers = $config_fileheaders;

                break;

            default:
                $error[] = 'Invalid file type configured';
                break;

        }

        if ($this->header_matched == true) {
            $logs[] = "<br/> Headers matched with the configuration!!";
        }

        $success = true;

        if (count($error) > 0) {
            $success = false;
        }

        echo "<pre>";
        if (count($logs) > 0) {
            print_r($logs);
        }

        if (count($error)) {
            print_r($error);
        }

        echo "</pre>";

        $result = array('error' => $error, 'success' => $success);

        //print_r(array('error' => $error, 'success' => $success));

        return $result;

    }

    public function read_csv_file_headers($args = array())
    {

        $file_path = $this->file_path;

        $row = 1;
        if (($file_handle = fopen($file_path, "r")) !== false) {
            while (($data = fgetcsv($file_handle, 1000, ",")) !== false) {
                if ($row == 1) {
                    $headers = $data;
                }

                $row++;
                if ($row == 2) {
                    break;
                }
            }
        }
        fclose($file_handle);

        return $headers;

    }

    /**
     * Replaces all line endings with '\n' in the provided file
     * @return     string  path of file
     */
    public function normalizeLineEndings()
    {
        $file_path = $this->file_path;

        $file_content = @file_get_contents($file_path);

        if (!$file_content) {
            return $file_path;
        }

        //normalize the all types of line ending to \n
        $file_content = preg_replace('~\r\n?~', "\n", $file_content);

        file_put_contents($file_path, $file_content);

        return $file_path;
    }

    public function getFileType()
    {

    }

    /**
     * Move the uploaded file to temporary location for processing
     */
    public function moveUploadedFile()
    {

    }

}
