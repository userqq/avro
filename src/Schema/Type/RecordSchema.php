<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Type;

use UserQQ\Avro\Datum\Encoder;
use UserQQ\Avro\Exception;
use UserQQ\Avro\Schema\Attribute;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Exception\TypeException;
use UserQQ\Avro\Schema\Field\Field;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Name\Name;
use UserQQ\Avro\Schema\Type;

final class RecordSchema extends NamedSchema
{
    private ?array $fieldsHash = null;

    /**
     * @throws ParseException
     */
    public function __construct(
        Name $name,
        ?string $doc,
        /** @var list<Field> */
        public readonly array $fields,
        Type $type = Type::RECORD,
        ?LogicalType $logicalType = null,
        array $extraAttributes = [],
    ) {
        if (Type::REQUEST === $type) {
            parent::__construct($type, $name, null);
        } else {
            parent::__construct($type, $name, $doc, $logicalType, $extraAttributes);
        }
    }

    public function getValue(): array
    {
        $avro = parent::getValue();

        $fieldsAvro = [];

        foreach ($this->fields as $field) {
            $fieldsAvro[] = $field->getValue();
        }

        if (Type::REQUEST === $this->type) {
            return $fieldsAvro;
        }

        $avro[Attribute::FIELDS->value] = $fieldsAvro;

        return $avro;
    }

    public function fieldsHash(): array
    {
        if (\is_null($this->fieldsHash)) {
            $hash = [];

            foreach ($this->fields as $field) {
                $hash[$field->name->name] = $field;
            }

            $this->fieldsHash = $hash;
        }

        return $this->fieldsHash;
    }

    public function isValidDatum(mixed $datum): bool
    {
        if (\is_array($datum)) {
            foreach ($this->fields as $field) {
                $value = $datum[$field->name->name] ?? $field->default;

                if (!$field->schema->isValidDatum($value)) {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    /**
     * @throws TypeException
     * @throws Exception
     */
    public function write(mixed $datum, Encoder $encoder): void
    {
        if (!$this->isValidDatum($datum)) {
            throw new TypeException($this, $datum);
        }

        foreach ($this->fields as $field) {
            $value = $datum[$field->name->name] ?? $field->default?->value;
            $field->schema->write($value, $encoder);
        }
    }
}
