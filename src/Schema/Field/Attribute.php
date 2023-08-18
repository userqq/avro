<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Field;

enum Attribute: string
{
    case NAME = 'name';

    case DEFAULT = 'default';

    case ORDER = 'order';

    case PRECISION = 'precision';

    case SCALE = 'scale';
}
