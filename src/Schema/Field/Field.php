<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Field;

use UserQQ\Avro\Schema;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Field\Attribute as FieldAttribute;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Name\Name;
use UserQQ\Avro\Schema\Value;

final class Field implements Value
{
    /**
     * @throws ParseException
     */
    public function __construct(
        public readonly Name $name,
        public readonly Schema $schema,
        public readonly ?DefaultValue $default = null,
        public readonly ?Order $order = null,
        public readonly ?string $doc = null,
        public readonly ?LogicalType $logicalType = null,
        public readonly ?int $precision = null,
        public readonly ?int $scale = null,
    ) {
    }

    public function getValue(): array
    {
        $avro = [FieldAttribute::NAME->value => $this->name->name];

        $avro[\UserQQ\Avro\Schema\Attribute::TYPE->value] = $this->schema->getValue();

        if (null !== $this->default) {
            $avro[FieldAttribute::DEFAULT->value] = $this->default->value;
        }

        if (null !== $this->order) {
            $avro[FieldAttribute::ORDER->value] = $this->order->value;
        }

        if (null !== $this->doc && '' !== $this->doc && '0' !== $this->doc) {
            $avro[\UserQQ\Avro\Schema\Attribute::DOC->value] = $this->doc;
        }

        if ($this->logicalType) {
            $avro[\UserQQ\Avro\Schema\Attribute::LOGICAL_TYPE->value] = $this->logicalType;
        }

        if (null !== $this->precision && 0 !== $this->precision) {
            $avro[FieldAttribute::PRECISION->value] = $this->precision;
        }

        if (null !== $this->scale && 0 !== $this->scale) {
            $avro[FieldAttribute::SCALE->value] = $this->scale;
        }

        return $avro;
    }
}
