<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Type;

use UserQQ\Avro\Datum\Encoder;
use UserQQ\Avro\Exception;
use UserQQ\Avro\Schema\Attribute;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Exception\TypeException;
use UserQQ\Avro\Schema\LogicalType;
use UserQQ\Avro\Schema\Name\Name;
use UserQQ\Avro\Schema\Type;

final class EnumSchema extends NamedSchema
{
    /**
     * @param list<string> $symbols
     * @throws ParseException
     */
    public function __construct(
        Name $name,
        ?string $doc,
        /** @var list<string> $symbols */
        public readonly array $symbols,
        ?LogicalType $logicalType = null,
        array $extraAttributes = [],
    ) {
        if (!\array_is_list($symbols)) {
            throw new ParseException('Enum Schema symbols are not a list');
        }

        if (\count(\array_unique($symbols)) > \count($symbols)) {
            throw new ParseException(\sprintf('Duplicate enum symbols: %s', \var_export($symbols, true)));
        }

        foreach ($symbols as $symbol) {
            /** @psalm-suppress DocblockTypeContradiction */
            if (!\is_string($symbol) || '' === $symbol) {
                throw new ParseException(\sprintf('Enum schema symbol must be a non-empty string %s', \var_export($symbol, true)));
            }
        }

        parent::__construct(Type::ENUM, $name, $doc, $logicalType, $extraAttributes);
    }

    public function hasSymbol(string $symbol): bool
    {
        return \in_array($symbol, $this->symbols);
    }

    /**
     * @throws Exception
     */
    public function symbolByIndex(int $index): string
    {
        return \array_key_exists($index, $this->symbols)
            ? $this->symbols[$index]
            : throw new Exception(\sprintf('Invalid symbol index %d', $index));
    }

    /**
     * @throws Exception
     */
    public function symbolIndex(string $symbol): int
    {
        $idx = \array_search($symbol, $this->symbols, true);

        return false !== $idx
            ? $idx
            : throw new Exception(\sprintf('Invalid symbol value "%s"', $symbol));
    }

    public function getValue(): array
    {
        $avro = parent::getValue();
        $avro[Attribute::SYMBOLS->value] = $this->symbols;

        return $avro;
    }

    public function isValidDatum(mixed $datum): bool
    {
        return \in_array($datum, $this->symbols);
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

        $index = $this->symbolIndex($datum);
        $encoder->writeInt($index);
    }
}
