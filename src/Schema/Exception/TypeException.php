<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Exception;

use UserQQ\Avro\Exception;

final class TypeException extends Exception
{
    public function __construct(mixed $expectedSchema, mixed $datum)
    {
        parent::__construct(\sprintf('The datum %s is not an example of schema %s', \var_export($datum, true), $expectedSchema));
    }
}
