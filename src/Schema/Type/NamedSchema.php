<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Type;

use UserQQ\Avro\Schema;
use UserQQ\Avro\Schema\Attribute;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Name\Name;
use UserQQ\Avro\Schema\Type;

abstract class NamedSchema extends Schema
{
    public readonly string $fullname;

    /**
     * @throws ParseException
     */
    public function __construct(
        Type $type,
        public readonly Name $name,
        public readonly ?string $doc,
        ?LogicalType $logicalType = null,
        array $extraAttributes = [],
    ) {
        $this->fullname = $name->fullname;

        parent::__construct($type, $logicalType, $extraAttributes);
    }

    public function getValue(): array
    {
        /** @var array $avro */
        $avro = parent::getValue();
        $avro[Attribute::NAME->value] = $this->name->name;

        if (null !== $this->name->namespace && '' !== $this->name->namespace && '0' !== $this->name->namespace) {
            $avro[Attribute::NAMESPACE->value] = $this->name->namespace;
        }

        if (null !== $this->doc) {
            $avro[Attribute::DOC->value] = $this->doc;
        }

        if (null !== $this->logicalType) {
            return \array_merge(
                $avro,
                [Attribute::LOGICAL_TYPE->value => $this->logicalType->value],
                $this->extraAttributes,
            );
        }

        return $avro;
    }
}
