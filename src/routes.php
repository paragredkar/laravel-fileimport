<?php

Route::get('calculator', function () {
    echo 'Hello from the calculator package!';
});

Route::get('/ajimportdata', 'AjFileImportController@importFiledataInDbTable');
Route::get('/childjobs', 'AjFileImportController@addChildRecordsJob');
Route::get('/isdirexists', 'AjFileImportController@isDirExists');
Route::get('/tablestructure', 'AjFileImportController@testTableStructure');
Route::post('/ajuploadfile', 'AjFileImportController@uploadFile');
Route::get('/ajvalidatefields', 'AjFileImportController@validateFields');


Route::get('/ajgetErrorLogs', 'AjFileImportController@getErrorLogs');

Route::get('/ajtestschedule', 'AjFileImportController@testSchedule');


Route::get('/ajimportfile', 'AjFileImportController@showUploadFile');

//Route::get('/laraqueue', 'AjFileImportController@send');



