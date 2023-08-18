<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Type;

use UserQQ\Avro\Schema;
use UserQQ\Avro\Schema\Attribute;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Type;

final class PrimitiveSchema extends Schema
{
    /**
     * @throws ParseException
     */
    public function __construct(
        Type $type,
        ?LogicalType $logicalType = null,
        array $extraAttributes = [],
        private readonly bool $serializeTypeAttribute = false,
    ) {
        return parent::__construct($type, $logicalType, $extraAttributes);
    }

    public function getValue(): array|string
    {
        if (null !== $this->logicalType) {
            /** @psalm-suppress PossiblyInvalidArgument */
            return \array_merge(
                parent::getValue(),
                [Attribute::LOGICAL_TYPE->value => $this->logicalType],
                $this->extraAttributes,
            );
        }

        if (true === $this->serializeTypeAttribute) {
            return [Attribute::TYPE->value => $this->type];
        }

        return $this->type->value;
    }
}
