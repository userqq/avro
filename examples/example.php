<?php

declare(strict_types=1);

use UserQQ\Avro\Codec;
use UserQQ\Avro\IO\StringBuffer;
use UserQQ\Avro\Reader;
use UserQQ\Avro\Schema;
use UserQQ\Avro\Writer;

require __DIR__ . '/../vendor/autoload.php';

$schema = Schema::parse(json_encode([
    'name' => 'member',
    'type' => 'record',
    'fields' => [
        ['name' => '__int', 'type' => 'int'],
        ['name' => '__long', 'type' => 'long'],
        ['name' => '__double', 'type' => 'double'],
        ['name' => '__string', 'type' => 'string'],
        ['name' => '__bytes', 'type' => 'bytes'],
        ['name' => '__enum', 'type' => 'enum', 'symbols' => ['__ENUM__']],
        ['name' => '__map', 'type' => 'map', 'values' => 'long'],
        ['name' => '__fixed', 'type' => 'fixed', 'size' => 16],
        ['name' => '__array', 'type' => 'array', 'items' => 'long'],
    ],
]));

$writeIO = new StringBuffer();
$writer = new Writer($writeIO, $schema, Codec::DEFLATE);

$writer->append([
    '__int'    => 1000,
    '__long'   => 100000000,
    '__double' => 99.99,
    '__string' => 'TEST',
    '__bytes'  => "\0\0\0\0\0",
    '__enum'   => '__ENUM__',
    '__map'    => ['value' => 1],
    '__fixed'  => 'AAAAAAAAAAAAAAAA',
    '__array'  => [1, 2, 3],
]);

$writer->close();

$readIO = new StringBuffer((string) $writeIO);
$reader = new Reader($readIO);

foreach ($reader as $record) {
    var_dump($record);
}
