<?php
/***************************************************************************
 * 
 * Copyright (c) 2015 koudai.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
require_once __DIR__.'/lib/Thrift/ClassLoader/ThriftClassLoader.php'; 
use Thrift\ClassLoader\ThriftClassLoader;
$GEN_DIR = realpath(dirname(__FILE__)).'/gen-php';
echo $GEN_DIR.PHP_EOL;

$loader = new ThriftClassLoader();
$loader->registerNamespace('Thrift', __DIR__ . '/lib');
//$loader->registerDefinition('shared', $GEN_DIR);
//$loader->registerDefinition('tutorial', $GEN_DIR);
$loader->register();

use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TSocket;
use Thrift\Transport\THttpClient;
use Thrift\Transport\TBufferedTransport;
use Thrift\Exception\TException;

require_once $GEN_DIR.'/THBaseService.php'; 
require_once $GEN_DIR.'/Types.php'; 

$table="nasa";

try{
    $socket = new TSocket( '10.1.3.61', 9090 );
    $socket->setSendTimeout (2000); 
    $socket->setRecvTimeout (4000); 
    $transport = new TBufferedTransport ($socket);
    $protocol = new TBinaryProtocol ($transport);
    $client = new THBaseServiceClient($protocol);
    //$client = new HbaseClient ($protocol);
    $transport->open ();
    scan();
//    write();
}
catch (TException $tx){
print 'TException: '.$tx->__toString(). ' Error: '.$tx->getMessage() . "\n";
}



function scan(){
    global $client;
    global $table;
    $columns = array("t");
    // $scanner = $client->scannerOpenWithStop( $table, "00020", "00040", $columns,array() );
    //"", $columns,array('versions'=>3)
    $tc = new TColumn(array('family'=>'t','qualifier'=>'url_action_name'));
    $atscan = array();
    $atscan['startRow'] = '';
    $atscan['maxVersions'] = 2;
    $atscan['columns'] = array($tc);
    $atscan['filterString'] ="PrefixFilter('1438041602http://weidian.com/wd/shop/diary/')";
    //$atscan['filterString'] ='*1438041602http://weidian.com/wd/shop/diary/viewShopDiary?id=1798568*';
    

    $tscan = new TScan($atscan);
    $scanner = $client->openScanner( $table,  $tscan);

    try {
        while (true){

            $get_arr = $client->getScannerRows( $scanner,1);
            if($get_arr == null){break;}
                print_r($get_arr);
            printRow( $get_arr[0] );
        }     
    } catch ( NotFound $nf ) {
        $client->scannerClose( $scanner );
        echo( "Scanner finished\n" );
    }

}

function write(){

    $f = fopen("url.log","r");
    while (($line = fgets($f)) !== false) {
        $line = trim($line);
        if (empty($line)) {
            continue;
        }   

        $res = explode("\t",$line);
        insert($res);
    }   
}
function insert($res){
global $client;
global $table;
$mutations = array(
  new Mutation( array(
    'column' => 't:url_action_name',
    'value' => $res[1],
    'versions' => 3
  ) ),
);

$value1 = $res[1];
$key = sprintf("%s%s",$res[0],$res[3]);
echo $key."--------".$value1.PHP_EOL;
$client->mutateRow( $table, $key, $mutations,array() );
}


function printRow( $rowresult ) {

//    print_r($rowresult);

  echo( "row: {$rowresult->row}, cols: \n" );
  $values = $rowresult->columnValues;
  asort( $values );
  foreach ( $values as $k=>$v ) {
    echo( "  {$k} => {$v->value}\n" );
  }
}



/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
?>
