<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema;

enum Type: string
{
    case NULL = 'null';

    case BOOLEAN = 'boolean';

    case INT = 'int';

    case LONG = 'long';

    case FLOAT = 'float';

    case DOUBLE = 'double';

    case STRING = 'string';

    case BYTES = 'bytes';

    case ARRAY = 'array';

    case MAP = 'map';

    case UNION = 'union';

    case ERROR_UNION = 'error_union';

    case ENUM = 'enum';

    case FIXED = 'fixed';

    case RECORD = 'record';

    case ERROR = 'error';

    case REQUEST = 'request';

    public const PRIMITIVE_TYPES = [
        self::BOOLEAN->value => self::BOOLEAN,
        self::BYTES->value => self::BYTES,
        self::DOUBLE->value => self::DOUBLE,
        self::FLOAT->value => self::FLOAT,
        self::INT->value => self::INT,
        self::LONG->value => self::LONG,
        self::NULL->value => self::NULL,
        self::STRING->value => self::STRING,
    ];

    public const NAMED_TYPES = [
        self::ENUM->value => self::ENUM,
        self::ERROR->value => self::ERROR,
        self::FIXED->value => self::FIXED,
        self::RECORD->value => self::RECORD,
    ];

    public function isPrimitive(): bool
    {
        return isset(static::PRIMITIVE_TYPES[$this->value]);
    }

    public function isNamed(): bool
    {
        return isset(static::NAMED_TYPES[$this->value]);
    }
}
