<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Field;

final class DefaultValue
{
    public function __construct(
        public readonly null|bool|int|float|string $value,
    ) {
    }
}
