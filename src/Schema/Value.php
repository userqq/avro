<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema;

interface Value
{
    public function getValue(): string|array;
}
