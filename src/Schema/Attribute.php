<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema;

enum Attribute: string
{
    case TYPE = 'type';

    case NAME = 'name';

    case NAMESPACE = 'namespace';

    case FULLNAME = 'fullname';

    case SIZE = 'size';

    case FIELDS = 'fields';

    case ITEMS = 'items';

    case SYMBOLS = 'symbols';

    case VALUES = 'values';

    case DOC = 'doc';

    case LOGICAL_TYPE = 'logicalType';

    public const RESERVED = [
        self::TYPE->value,
        self::NAME->value,
        self::NAMESPACE->value,
        self::FULLNAME->value,
        self::SIZE->value,
        self::FIELDS->value,
        self::ITEMS->value,
        self::SYMBOLS->value,
        self::VALUES->value,
        self::DOC->value,
        self::LOGICAL_TYPE->value,
    ];
}
