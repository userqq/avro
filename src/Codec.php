<?php

declare(strict_types=1);

namespace UserQQ\Avro;

enum Codec: string
{
    case NULL = 'null';

    case DEFLATE = 'deflate';

    case SNAPPY = 'snappy';
}
