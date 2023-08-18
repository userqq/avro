<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Field;

enum Order: string
{
    case ASCENDING = 'ascending';

    case DESCENDING = 'descending';

    case IGNORE = 'ignore';
}
