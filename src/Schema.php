<?php

declare(strict_types=1);

namespace UserQQ\Avro;

use UserQQ\Avro\Schema\AbstractValue;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Field\Attribute as FieldAttribute;
use UserQQ\Avro\Schema\Field\DefaultValue;
use UserQQ\Avro\Schema\Field\Field;
use UserQQ\Avro\Schema\Field\Order;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Name\Name;
use UserQQ\Avro\Schema\Type;
use UserQQ\Avro\Schema\Type\ArraySchema;
use UserQQ\Avro\Schema\Type\EnumSchema;
use UserQQ\Avro\Schema\Type\FixedSchema;
use UserQQ\Avro\Schema\Type\MapSchema;
use UserQQ\Avro\Schema\Type\PrimitiveSchema;
use UserQQ\Avro\Schema\Type\RecordSchema;
use UserQQ\Avro\Schema\Type\UnionSchema;
use UserQQ\Avro\Schema\Value;

class Schema extends AbstractValue implements Value, \Stringable
{
    /**
     * @throws ParseException
     */
    public static function parse(string $json): self
    {
        try {
            $avro = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new ParseException($e->getMessage(), $e->getCode(), $e);
        }

        return self::realParse($avro);
    }

    /**
     * @throws ParseException
     */
    private static function realParse(
        mixed $avro,
        ?string $defaultNamespace = null,
    ): self {
        if (\is_string($avro) && (null !== $type = Type::tryFrom($avro)) && $type->isPrimitive()) {
            return new PrimitiveSchema($type, null, [], true);
        } elseif (\is_array($avro)) {
            if (!\array_key_exists(\UserQQ\Avro\Schema\Attribute::TYPE->value, $avro) && \array_is_list($avro)) {
                return new UnionSchema(\array_map(static fn ($avro) => self::subparse($avro, $defaultNamespace), $avro));
            } elseif (null !== $type = Type::tryFrom($avro[\UserQQ\Avro\Schema\Attribute::TYPE->value])) {
                $logicalType = isset($avro[\UserQQ\Avro\Schema\Attribute::LOGICAL_TYPE->value])
                    ? LogicalType::from($avro[\UserQQ\Avro\Schema\Attribute::LOGICAL_TYPE->value])
                    : null;

                $extraAttributes = \array_diff_key($avro, \array_flip(\UserQQ\Avro\Schema\Attribute::RESERVED));

                if ($type->isPrimitive()) {
                    return new PrimitiveSchema($type, $logicalType, $extraAttributes, true);
                } elseif ($type->isNamed()) {
                    $name = $avro[\UserQQ\Avro\Schema\Attribute::NAME->value] ?? null;
                    if (!\is_string($name)) {
                        throw new ParseException(\sprintf('Named %s has not or has incorrect name value in %s.', $type, \var_export($avro, true)));
                    }
                    $namespace = $avro[\UserQQ\Avro\Schema\Attribute::NAMESPACE->value] ?? null;
                    $avroName = new Name($name, $namespace, $defaultNamespace);
                    $doc = $avro[\UserQQ\Avro\Schema\Attribute::DOC->value] ?? null;

                    /** @psalm-suppress ArgumentTypeCoercion */
                    return match ($type) {
                        Type::FIXED => \is_int($size = $avro[\UserQQ\Avro\Schema\Attribute::SIZE->value] ?? null)
                            ? new FixedSchema($avroName, $doc, $size, $logicalType, $extraAttributes)
                            : throw new ParseException(\sprintf('%s has not or has incorrect size value in %s.', $type, \var_export($avro, true))),

                        Type::ENUM => \is_array($symbols = $avro[\UserQQ\Avro\Schema\Attribute::SYMBOLS->value] ?? null)
                            ? new EnumSchema($avroName, $doc, $symbols, $logicalType, $extraAttributes)
                            : throw new ParseException(\sprintf('%s has not or has incorrect symbols value in %s.', $type, \var_export($avro, true))),

                        Type::RECORD,
                        Type::ERROR => \is_array($fields = $avro[\UserQQ\Avro\Schema\Attribute::FIELDS->value] ?? null)
                            ? new RecordSchema($avroName, $doc, self::parseFields($fields, $avroName->namespace), $type, $logicalType, $extraAttributes)
                            : throw new ParseException(\sprintf('%s has not or has incorrect symbols value in %s.', $type, \var_export($avro, true))),

                        default => throw new ParseException(\sprintf('Unknown named type: %s', $type)),
                    };
                } else {
                    return match ($type) {
                        Type::ARRAY => new ArraySchema(self::subparse($avro[\UserQQ\Avro\Schema\Attribute::ITEMS->value] ?? null, $defaultNamespace), $logicalType, $extraAttributes),
                        Type::MAP => new MapSchema(self::subparse($avro[\UserQQ\Avro\Schema\Attribute::VALUES->value] ?? null, $defaultNamespace), $logicalType, $extraAttributes),
                        default => throw new ParseException(\sprintf('Unknown valid type: %s', $type)),
                    };
                }
            } else {
                throw new ParseException(\sprintf('Undefined type in %s', \var_export($avro, true)));
            }
        }

        throw new ParseException(\sprintf('%s is not a schema we know about.', \var_export($avro, true)));
    }

    /**
     * @return list<Field>
     * @throws ParseException
     */
    private static function parseFields(array $fieldData, ?string $defaultNamespace): array
    {
        $fields = [];
        $fieldNames = [];

        foreach ($fieldData as $field) {
            $name = $field[FieldAttribute::NAME->value]
                ?? throw new ParseException(\sprintf('Field name is missing in field %s', \var_export($field, true)));

            if (\in_array($name, $fieldNames)) {
                throw new ParseException(\sprintf('Field name %s is already in use', $name));
            }

            $typeValue = $field[\UserQQ\Avro\Schema\Attribute::TYPE->value]
                ?? throw new ParseException(\sprintf('Field %s\'s type is missing', $name));

            $schema = (\is_string($typeValue) && (null !== $type = Type::tryFrom($typeValue)) && !$type->isPrimitive())
                ? self::subparse($field, $defaultNamespace)
                : self::subparse($typeValue, $defaultNamespace);

            try {
                $logicalType = \array_key_exists(\UserQQ\Avro\Schema\Attribute::LOGICAL_TYPE->value, $field)
                    ? LogicalType::from($field[\UserQQ\Avro\Schema\Attribute::LOGICAL_TYPE->value])
                    : null;
            } catch (\ValueError) {
                throw new ParseException(\sprintf('Logical type for field %s has incorrect value %s.', $name, \var_export($field[\UserQQ\Avro\Schema\Attribute::LOGICAL_TYPE->value], true)));
            }

            $default = \array_key_exists(FieldAttribute::DEFAULT->value, $field)
                ? new DefaultValue($field[FieldAttribute::DEFAULT->value])
                : null;

            try {
                $order = \array_key_exists(FieldAttribute::ORDER->value, $field)
                    ? Order::from($field[FieldAttribute::ORDER->value] ?? '')
                    : null;
            } catch (\ValueError) {
                throw new ParseException(\sprintf('Order for field %s has incorrect value %s.', $name, \var_export($field[FieldAttribute::ORDER->value], true)));
            }

            $doc = $field[\UserQQ\Avro\Schema\Attribute::DOC->value] ?? null;
            $precision = $field[FieldAttribute::PRECISION->value] ?? null;
            $scale = $field[FieldAttribute::SCALE->value] ?? null;

            $fields[] = new Field(new Name($name), $schema, $default, $order, $doc, $logicalType, $precision, $scale);
            $fieldNames[] = $name;
        }

        return $fields;
    }

    /**
     * @throws ParseException
     */
    final protected static function subparse(
        mixed $avro,
        ?string $defaultNamespace,
    ): self {
        try {
            return self::realParse($avro, $defaultNamespace);
        } catch (ParseException $e) {
            throw $e;
        } catch (Exception) {
            throw new ParseException(\sprintf('Sub-schema is not a valid Avro schema: %s', \var_export($avro, true)));
        }
    }

    protected function __construct(
        public readonly Type $type,
        public readonly ?LogicalType $logicalType = null,
        public readonly array $extraAttributes = [],
    ) {
    }

    public function getValue(): string|array
    {
        return [\UserQQ\Avro\Schema\Attribute::TYPE->value => $this->type];
    }

    public function __toString(): string
    {
        return \json_encode($this->getValue(), \JSON_THROW_ON_ERROR);
    }
}
