<?php

declare(strict_types=1);

namespace UserQQ\Avro\Datum;

use UserQQ\Avro\Exception;
use UserQQ\Avro\Schema;
use UserQQ\Avro\Schema\Attribute;
use UserQQ\Avro\Schema\Type;
use UserQQ\Avro\Schema\Type\ArraySchema;
use UserQQ\Avro\Schema\Type\EnumSchema;
use UserQQ\Avro\Schema\Type\FixedSchema;
use UserQQ\Avro\Schema\Type\MapSchema;
use UserQQ\Avro\Schema\Type\RecordSchema;
use UserQQ\Avro\Schema\Type\UnionSchema;

final class Reader
{
    private static function schemasMatch(Schema $writersSchema, Schema $readersSchema): bool
    {
        $writersSchemaType = $writersSchema->type;
        $readersSchemaType = $readersSchema->type;

        if (Type::UNION === $writersSchemaType || Type::UNION === $readersSchemaType) {
            return true;
        }

        if ($writersSchemaType === $readersSchemaType) {
            if ($writersSchemaType->isPrimitive()) {
                return true;
            }

            switch ($readersSchemaType) {
                case Type::MAP:
                    /** @psalm-suppress UndefinedPropertyFetch */
                    return self::attributesMatch($writersSchema->values, $readersSchema->values, [Attribute::TYPE]);
                case Type::ARRAY:
                    /** @psalm-suppress UndefinedPropertyFetch */
                    return self::attributesMatch($writersSchema->items, $readersSchema->items, [Attribute::TYPE]);
                case Type::ENUM:
                    return self::attributesMatch($writersSchema, $readersSchema, [Attribute::FULLNAME]);
                case Type::FIXED:
                    return self::attributesMatch($writersSchema, $readersSchema, [Attribute::FULLNAME, Attribute::SIZE]);
                case Type::RECORD:
                case Type::ERROR:
                    return self::attributesMatch($writersSchema, $readersSchema, [Attribute::FULLNAME]);
                case Type::REQUEST:
                    // XXX: This seems wrong
                    return true;
                // XXX: no default
            }

            if (Type::INT === $writersSchemaType && \in_array($readersSchemaType, [Type::LONG, Type::FLOAT, Type::DOUBLE])) {
                return true;
            }

            if (Type::LONG === $writersSchemaType && \in_array($readersSchemaType, [Type::FLOAT, Type::DOUBLE])) {
                return true;
            }

            return Type::FLOAT === $writersSchemaType && Type::DOUBLE === $readersSchemaType;
        }

        return false;
    }

    /**
     * @param list<Attribute> $attributes
     */
    private static function attributesMatch(Schema $one, Schema $two, array $attributes): bool
    {
        foreach ($attributes as $attribute) {
            if ($one->{$attribute->value} !== $two->{$attribute->value}) {
                return false;
            }
        }

        return true;
    }

    /**
     * @throws Exception
     */
    public static function skipData(Schema $writersSchema, Decoder $avroIOBinaryDecoder): void
    {
        /** @psalm-suppress ArgumentTypeCoercion */
        match ($writersSchema->type) {
            Type::NULL => $avroIOBinaryDecoder->skipNull(),
            Type::BOOLEAN => $avroIOBinaryDecoder->skipBoolean(),
            Type::INT => $avroIOBinaryDecoder->skipInt(),
            Type::LONG => $avroIOBinaryDecoder->skipLong(),
            Type::FLOAT => $avroIOBinaryDecoder->skipFloat(),
            Type::DOUBLE => $avroIOBinaryDecoder->skipDouble(),
            Type::STRING => $avroIOBinaryDecoder->skipString(),
            Type::BYTES => $avroIOBinaryDecoder->skipBytes(),
            Type::ARRAY => $avroIOBinaryDecoder->skipArray($writersSchema, $avroIOBinaryDecoder),
            Type::MAP => $avroIOBinaryDecoder->skipMap($writersSchema, $avroIOBinaryDecoder),
            Type::UNION => $avroIOBinaryDecoder->skipUnion($writersSchema, $avroIOBinaryDecoder),
            Type::ENUM => $avroIOBinaryDecoder->skipEnum($writersSchema, $avroIOBinaryDecoder),
            Type::FIXED => $avroIOBinaryDecoder->skipFixed($writersSchema, $avroIOBinaryDecoder),

            Type::RECORD,
            Type::ERROR,
            Type::REQUEST => $avroIOBinaryDecoder->skipRecord($writersSchema, $avroIOBinaryDecoder),

            default => throw new Exception(\sprintf('Uknown schema type: %s', $writersSchema->type)),
        };
    }

    public function __construct(private ?Schema $writersSchema = null, private ?Schema $readersSchema = null)
    {
    }

    public function setWritersSchema(Schema $readersSchema): void
    {
        $this->writersSchema = $readersSchema;
    }

    /**
     * @throws Exception
     * @throws SchemaMatchException
     */
    public function read(Decoder $decoder): mixed
    {
        if (null === $this->readersSchema) {
            $this->readersSchema = $this->writersSchema;
        }

        /** @psalm-suppress PossiblyNullArgument */
        return $this->readData($this->writersSchema, $this->readersSchema, $decoder);
    }

    /**
     * @throws Exception
     * @throws SchemaMatchException
     */
    public function readData(Schema $writersSchema, Schema $readersSchema, Decoder $decoder): mixed
    {
        if (!self::schemasMatch($writersSchema, $readersSchema)) {
            throw new SchemaMatchException($writersSchema, $readersSchema);
        }

        // Schema resolution: reader's schema is a union, writer's schema is not
        /** @psalm-suppress UndefinedPropertyFetch */
        if (Type::UNION === $readersSchema->type && Type::UNION !== $writersSchema->type) {
            foreach ($readersSchema->schemas as $schema) {
                if (self::schemasMatch($writersSchema, $schema)) {
                    return $this->readData($writersSchema, $schema, $decoder);
                }
            }

            throw new SchemaMatchException($writersSchema, $readersSchema);
        }

        /** @psalm-suppress ArgumentTypeCoercion */
        return match ($writersSchema->type) {
            Type::NULL => $decoder->readNull(),
            Type::BOOLEAN => $decoder->readBoolean(),
            Type::INT => $decoder->readInt(),
            Type::LONG => $decoder->readLong(),
            Type::FLOAT => $decoder->readFloat(),
            Type::DOUBLE => $decoder->readDouble(),
            Type::STRING => $decoder->readString(),
            Type::BYTES => $decoder->readBytes(),
            Type::ARRAY => $this->readArray($writersSchema, $readersSchema, $decoder),
            Type::MAP => $this->readMap($writersSchema, $readersSchema, $decoder),
            Type::UNION => $this->readUnion($writersSchema, $readersSchema, $decoder),
            Type::ENUM => $this->readEnum($writersSchema, $readersSchema, $decoder),
            Type::FIXED => $this->readFixed($writersSchema, $readersSchema, $decoder),

            Type::RECORD,
            Type::ERROR,
            Type::REQUEST => $this->readRecord($writersSchema, $readersSchema, $decoder),

            default => throw new Exception(\sprintf('Cannot read unknown schema type: %s', $writersSchema->type)),
        };
    }

    /**
     * @throws Exception
     * @throws SchemaMatchException
     */
    private function readArray(ArraySchema $writersSchema, ArraySchema $readersSchema, Decoder $decoder): array
    {
        $items = [];
        $blockCount = $decoder->readLong();
        while (0 !== $blockCount) {
            if ($blockCount < 0) {
                $blockCount = -$blockCount;
                // Read (and ignore) block size
                $decoder->readLong();
            }

            for ($i = 0; $i < $blockCount; ++$i) {
                $items[] = $this->readData($writersSchema->items, $readersSchema->items, $decoder);
            }

            $blockCount = $decoder->readLong();
        }

        return $items;
    }

    /**
     * @throws Exception
     * @throws SchemaMatchException
     */
    private function readMap(MapSchema $writersSchema, MapSchema $readersSchema, Decoder $decoder): array
    {
        $items = [];
        $pairCount = $decoder->readLong();
        while (0 !== $pairCount) {
            if ($pairCount < 0) {
                $pairCount = -$pairCount;
                // Note: Ingoring what we read here
                $decoder->readLong();
            }

            for ($i = 0; $i < $pairCount; ++$i) {
                $key = $decoder->readString();
                $items[$key] = $this->readData($writersSchema->values, $readersSchema->values, $decoder);
            }

            $pairCount = $decoder->readLong();
        }

        return $items;
    }

    /**
     * @throws Exception
     * @throws SchemaMatchException
     */
    private function readUnion(UnionSchema $writersSchema, UnionSchema $readersSchema, Decoder $decoder): mixed
    {
        $schemaIndex = $decoder->readLong();
        $selectedWritersSchema = $writersSchema->schemaByIndex($schemaIndex);

        return $this->readData($selectedWritersSchema, $readersSchema, $decoder);
    }

    /**
     * @throws Exception
     */
    private function readEnum(EnumSchema $writersSchema, Schema $readersSchema, Decoder $decoder): string
    {
        $symbolIndex = $decoder->readInt();

        return $writersSchema->symbolByIndex($symbolIndex);
    }

    private function readFixed(FixedSchema $writersSchema, FixedSchema $readersSchema, Decoder $decoder): string
    {
        return $decoder->read($writersSchema->size());
    }

    /**
     * @throws Exception
     * @throws SchemaMatchException
     */
    private function readRecord(RecordSchema $writersSchema, RecordSchema $readersSchema, Decoder $decoder): array
    {
        $readersFields = $readersSchema->fieldsHash();
        $record = [];
        foreach ($writersSchema->fields as $writersFields) {
            if (isset($readersFields[$writersFields->name->name])) {
                $record[$writersFields->name->name] = $this->readData($writersFields->schema, $readersFields[$writersFields->name->name]->schema, $decoder);
            } else {
                self::skipData($writersFields->schema, $decoder);
            }
        }

        // Fill in default values
        /** @psalm-suppress RedundantCondition, TypeDoesNotContainType */
        if ((\is_countable($readersFields) ? \count($readersFields) : 0) > \count($record)) {
            $writersFields = $writersSchema->fieldsHash();
            foreach ($readersFields as $fieldName => $field) {
                if (!isset($writersFields[$fieldName])) {
                    if (null !== $field->default) {
                        $record[$field->name()] = $this->readDefaultValue($field->type, $field->default->value);
                    } else {
                        // should throw an exception since no data and default value is not passed ?
                    }
                }
            }
        }

        return $record;
    }

    /**
     * @throws Exception
     */
    private function readDefaultValue(Schema $schema, mixed $defaultValue): mixed
    {
        switch ($schema->type) {
            case Type::NULL:
                return null;
            case Type::BOOLEAN:
                return $defaultValue;
            case Type::INT:
            case Type::LONG:
                return (int) $defaultValue;
            case Type::FLOAT:
            case Type::DOUBLE:
                return (float) $defaultValue;
            case Type::STRING:
            case Type::BYTES:
                return $defaultValue;
            case Type::ARRAY:
                /** @var ArraySchema $schema */
                $array = [];
                foreach ($defaultValue as $jsonValue) {
                    $val = $this->readDefaultValue($schema->items, $jsonValue);
                    $array[] = $val;
                }

                return $array;
            case Type::MAP:
                /** @var MapSchema $schema */
                $map = [];
                foreach ($defaultValue as $key => $jsonValue) {
                    $map[$key] = $this->readDefaultValue($schema->values, $jsonValue);
                }

                return $map;
            case Type::UNION:
                /** @var UnionSchema $schema */
                return $this->readDefaultValue($schema->schemaByIndex(0), $defaultValue);
            case Type::ENUM:
            case Type::FIXED:
                return $defaultValue;
            case Type::RECORD:
                /** @var RecordSchema $schema */
                $record = [];
                foreach ($schema->fields as $field) {
                    $fieldName = $field->name->name;
                    if (!($jsonValue = $defaultValue[$fieldName])) {
                        $jsonValue = $field->default?->value;
                        // should throw and exception if no default value ?
                    }

                    $record[$fieldName] = $this->readDefaultValue($field->schema, $jsonValue);
                }

                return $record;
            default:
                throw new Exception(\sprintf('Unknown type: %s', $schema->type));
        }
    }
}
