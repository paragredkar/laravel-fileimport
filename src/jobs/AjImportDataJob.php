<?php

namespace Ajency\Ajfileimport\jobs;

use Ajency\Ajfileimport\Helpers\AjCsvFileImport;
use App\Jobs;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;

//Additional Dependencies
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Log;

class AjImportDataJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $params;
    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct($params)
    {
        //
        $this->params = $params;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {

        //
        /*$mailer->send('email.welcome', ['data'=>'data'], function ($message) {

        $message->from('parag@ajency.in', 'Christian Nwmaba');

        $message->to('paragredkar@gmail.com');

        });*/

        $aj_file_import = new AjCsvFileImport();
        Log::info('JOB params');
        Log::info($this->params);

        switch ($this->params['type']) {

            case 'validateunique':$aj_file_import->processUniqueFieldValidationQueue($this->params);
                Log::info('Processing validateunique ');

                break;
            case 'insert_records':$aj_file_import->addInsertRecordsQueue($this->params);
                Log::info('Processing insert_records ');

                break;

           /* case 'validatechildinsert':$aj_file_import->processTempTableFieldValidation($this->params);

                Log::info('Processing validatechildinsert ');

                break;
            case 'insertvalidchilddata':$aj_file_import->exportValidTemptableDataToFile($this->params);

                Log::info('Processing insertvalidchilddata ');

                break;

            case 'tempupdatechildid':$aj_file_import->UpdateTempTableWithChildInsertIds($this->params);
                Log::info('Processing tempupdatechildid ');

                break;
            case 'masterinsert':$aj_file_import->process_masterinsert_queue($this->params);
                Log::info('Processing masterinsert ');*/

                break;

        }

    }
}
