<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema;

use UserQQ\Avro\Datum\Encoder;
use UserQQ\Avro\Exception;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Exception\TypeException;

abstract class AbstractValue
{
    private const INT_MIN_VALUE = -2_147_483_648;
    private const INT_MAX_VALUE = 2_147_483_647;
    private const LONG_MIN_VALUE = -9_223_372_036_854_775_808.0;
    private const LONG_MAX_VALUE = 9_223_372_036_854_775_807;

    public readonly Type $type;

    /**
     * @throws ParseException
     */
    public function isValidDatum(mixed $datum): bool
    {
        return match ($this->type) {
            Type::NULL => \is_null($datum),
            Type::BOOLEAN => \is_bool($datum),

            Type::STRING,
            Type::BYTES => \is_string($datum),

            Type::INT => \is_int($datum) && self::INT_MIN_VALUE <= $datum && $datum <= self::INT_MAX_VALUE,
            Type::LONG => \is_int($datum) && self::LONG_MIN_VALUE <= $datum && $datum <= self::LONG_MAX_VALUE,

            Type::FLOAT,
            Type::DOUBLE => \is_float($datum) || \is_int($datum),

            default => throw new ParseException(\sprintf('Unknown type %s is not allowed.', \var_export($this->type, true))),
        };
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

        match ($this->type) {
            Type::NULL => $encoder->writeNull($datum),
            Type::BOOLEAN => $encoder->writeBoolean($datum),
            Type::INT => $encoder->writeInt($datum),
            Type::LONG => $encoder->writeLong($datum),
            Type::FLOAT => $encoder->writeFloat($datum),
            Type::DOUBLE => $encoder->writeDouble($datum),
            Type::STRING => $encoder->writeString($datum),
            Type::BYTES => $encoder->writeBytes($datum),
            default => throw new Exception(\sprintf('Uknown type: %s', $this->type)),
        };
    }
}
