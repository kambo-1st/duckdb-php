<?php

// Quick and dirty memory leak detection in PHP

if (!isset($argv[1])) {
    echo "Error: Folder path is not provided.\n";
    exit(1); // Return error code 1
}

function createTempFolder($prefix = 'temp', $mode = 0777)
{
    $folder = sys_get_temp_dir() . DIRECTORY_SEPARATOR . $prefix . uniqid();

    if (!is_dir($folder)) {
        mkdir($folder, $mode, true);
    }

    return $folder;
}

$folder = $argv[1];
$xml_file = createTempFolder('leaks-tests') . DIRECTORY_SEPARATOR . 'valgrind_results.xml';

$errorsFinal = [];
// Run valgrind for each PHP script in the folder
$files = glob("$folder/*.php");
foreach ($files as $file) {
    $cmd = "valgrind --tool=memcheck --xml=yes --xml-file=$xml_file php $file > /dev/null 2>&1";
    system($cmd);

    // Load the XML file
    $xml = simplexml_load_file($xml_file);

    // Check if there are any errors
    $errors = $xml->xpath('//error');
    if (count($errors) > 0) {
        // Print the error details
        foreach ($errors as $error) {
            $errorsFinal[] = $file . ' Error ' . $error->unique . ': ' . $error->kind . ' - ' . $error->xwhat->text;
        }
    }
}

if (count($errorsFinal) > 0) {
    echo "**** Failed **** \n";
    echo implode("\n", $errorsFinal);
    exit(2);
} else {
    echo 'No leaks found.';
}
