<?php

 
$ajimport_config['filetype']  = "csv";
$ajimport_config['delimiter'] = ",";
$ajimport_config['batchsize'] = "1000";


$ajimport_config['fileheader'] = array('seq', 'first', 'last', 'age', 'street', 'city', 'state', 'zip', 'email');

/* Final table on which insertion will be done, 
 * name  - master table name
 * field_map - {temp tablefieldname or header column name =>corresponding master table field name }   
 */

$ajimport_config['mastertable'] = ['name' => 'finaldata',

    'fields_map' => ["seq" => "f_no", "first" => "f_fname", "last"  => "f_lname",
        "age"=> "f_age", "street" => "f_street", "city" => "f_city",
          "zip"  => "f_zip" 
    ],
];


/* Add Child tables here */

/* 'csvfield' is the column that will be added on updated based on insertin to child table.
childfield is the coumn of child table that will added for corresponding record on csv data*/
$ajimport_config['childtables'][] = [
    'name'                => 'testuser',    
    'insertid_childtable' => 'id',
    'insertid_mtable'     => 'f_userid', //master table map    
    'fields_map'          => ["email" => "uemail"], //'temp table field'=>'child table field'
    //'insertid_temptable'  => 'userid', // 'Field to be added to temp table to store id of insertion record to child table'
    //'insertid_temptable'=> array('userid' => 'id'),
];

$ajimport_config['childtables'][] = array(
'name'                => 'states',
'insertid_childtable' => 'id',
'insertid_mtable'     => 'f_stateid' ,
'fields_map'          => array("state" => "name"), //'temp table field'=>'child table field'
//'insertid_temptable'=> array('userid' => 'id'),                                                   
//'insertid_temptable'  => 'stateid', // 'Field to be added to temp table to store id of insertion record to child table'                                                   
);

/* End Add Child tables here */

$ajimport_config['temptablename'] = $ajimport_config['mastertable']['name'] . '_temp';
//$ajimport_config['filepath']  = resource_path('uploads') . "/filetoimport.csv";
 

return $ajimport_config;
