<?php

declare(strict_types=1);

namespace UserQQ\Avro\Datum;

use UserQQ\Avro\Exception as Base;

final class SchemaMatchException extends Base
{
    public function __construct(mixed $writersSchema, mixed $readersSchema)
    {
        parent::__construct(\sprintf(
            'Writer\'s schema %s and Reader\'s schema %s do not match.',
            \var_export($writersSchema, true),
            \var_export($readersSchema, true),
        ));
    }
}
