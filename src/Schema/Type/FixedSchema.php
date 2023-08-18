<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Type;

use UserQQ\Avro\Datum\Encoder;
use UserQQ\Avro\Schema\Attribute;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Exception\TypeException;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Name\Name;
use UserQQ\Avro\Schema\Type;

final class FixedSchema extends NamedSchema
{
    /**
     * @throws ParseException
     */
    public function __construct(
        Name $name,
        ?string $doc,
        public readonly int $size,
        ?LogicalType $logicalType = null,
        array $extraAttributes = [],
    ) {
        // Fixed schemas don't have doc strings.
        $doc = null;

        parent::__construct(Type::FIXED, $name, $doc, $logicalType, $extraAttributes);
    }

    public function size(): int
    {
        return $this->size;
    }

    public function getValue(): array
    {
        $avro = parent::getValue();
        $avro[Attribute::SIZE->value] = $this->size;

        return $avro;
    }

    public function isValidDatum(mixed $datum): bool
    {
        return \is_string($datum) && \strlen($datum) === $this->size;
    }

    /**
     * @throws ParseException
     * @throws TypeException
     */
    public function write(mixed $datum, Encoder $encoder): void
    {
        if (!$this->isValidDatum($datum)) {
            throw new TypeException($this, $datum);
        }

        $encoder->write($datum);
    }
}
