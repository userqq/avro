<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema;

use UserQQ\Avro\Schema;
use UserQQ\Avro\Schema\Type\MapSchema;
use UserQQ\Avro\Schema\Type\PrimitiveSchema;

final class Metadata
{
    public const VERSION = 1;

    public const SYNC_SIZE = 16;

    public const SYNC_INTERVAL = 64_000;

    public const CODEC_ATTRIBUTE = "avro.codec";

    public const SCHEMA_ATTRIBUTE = "avro.schema";

    public static function magic(): string
    {
        return 'Obj' . \pack('c', self::VERSION);
    }

    public static function magicSize(): int
    {
        return \strlen(self::magic());
    }

    public static function schema(): Schema
    {
        static $schema;

        return $schema ??= new MapSchema(new PrimitiveSchema(Type::BYTES, null, [], true));
    }
}
