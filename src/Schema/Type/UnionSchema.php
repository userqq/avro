<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Type;

use UserQQ\Avro\Datum\Encoder;
use UserQQ\Avro\Exception;
use UserQQ\Avro\Schema;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Exception\TypeException;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Type;

final class UnionSchema extends Schema
{
    /**
     * @throws ParseException
     */
    public function __construct(
        /** @var list<Schema> */
        public readonly array $schemas,
        ?LogicalType $logicalType = null,
        array $extraAttributes = [],
    ) {
        parent::__construct(Type::UNION, $logicalType, $extraAttributes);

        $schemaTypes = [];
        foreach ($schemas as $schema) {
            if (!$schema->type->isNamed() && \in_array($schema->type, $schemaTypes)) {
                throw new ParseException(\sprintf('%s is already in union', \var_export($schema->type, true)));
            }

            if (Type::UNION === $schema->type) {
                throw new ParseException('Unions cannot contain other unions');
            }

            $schemaTypes[] = $schema->type;
        }
    }

    public function schemaByIndex(int $index): Schema
    {
        return \count($this->schemas) > $index
            ? $this->schemas[$index]
            : throw new ParseException('Invalid union schema index');
    }

    public function getValue(): array
    {
        /** @var list<string|array> */
        $avro = [];
        foreach ($this->schemas as $schema) {
            $avro[] = $schema->getValue();
        }

        return $avro;
    }

    public function isValidDatum(mixed $datum): bool
    {
        foreach ($this->schemas as $schema) {
            if ($schema->isValidDatum($datum)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @throws Exception
     * @throws TypeException
     */
    public function write(mixed $datum, Encoder $encoder): void
    {
        if (!$this->isValidDatum($datum)) {
            throw new TypeException($this, $datum);
        }

        $datumSchemaIndex = -1;
        $datumSchema = null;

        foreach ($this->schemas as $index => $schema) {
            if ($schema->isValidDatum($datum)) {
                $datumSchemaIndex = $index;
                $datumSchema = $schema;

                break;
            }
        }

        if (null === $datumSchema) {
            throw new TypeException($this->schemas, $datum);
        }

        $encoder->writeLong($datumSchemaIndex);
        $datumSchema->write($datum, $encoder);
    }
}
