<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Type;

use UserQQ\Avro\Datum\Encoder;
use UserQQ\Avro\Schema;
use UserQQ\Avro\Schema\Attribute;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Exception\TypeException;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Type;

final class ArraySchema extends Schema
{
    public function __construct(
        public readonly Schema $items,
        ?LogicalType $logicalType = null,
        array $extraAttributes = [],
    ) {
        parent::__construct(Type::ARRAY, $logicalType, $extraAttributes);
    }

    public function getValue(): array
    {
        /** @var array $avro */
        $avro = parent::getValue();
        $avro[Attribute::ITEMS->value] = $this->items->getValue();

        if (null !== $this->logicalType) {
            return \array_merge(
                $avro,
                [Attribute::LOGICAL_TYPE->value => $this->logicalType],
                $this->extraAttributes,
            );
        }

        return $avro;
    }

    public function isValidDatum(mixed $datum): bool
    {
        if (\is_array($datum)) {
            foreach ($datum as $d) {
                if (!$this->items->isValidDatum($d)) {
                    return false;
                }
            }

            return true;
        }

        return false;
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

        $datumCount = \is_countable($datum) ? \count($datum) : 0;
        if (0 < $datumCount) {
            $encoder->writeLong($datumCount);
            $items = $this->items;
            /** @psalm-suppress PossibleRawObjectIteration */
            foreach ($datum as $item) {
                $items->write($item, $encoder);
            }
        }

        $encoder->writeLong(0);
    }
}
