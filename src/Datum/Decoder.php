<?php

declare(strict_types=1);

namespace UserQQ\Avro\Datum;

use UserQQ\Avro\Exception;
use UserQQ\Avro\IO\IO;
use UserQQ\Avro\Schema\Type\ArraySchema;
use UserQQ\Avro\Schema\Type\EnumSchema;
use UserQQ\Avro\Schema\Type\FixedSchema;
use UserQQ\Avro\Schema\Type\MapSchema;
use UserQQ\Avro\Schema\Type\RecordSchema;
use UserQQ\Avro\Schema\Type\UnionSchema;

final class Decoder
{
    public static function decodeLongFromArray(array $bytes): int
    {
        $b = \array_shift($bytes);
        $n = $b & 0x7f;
        $shift = 7;

        while (0 !== ($b & 0x80)) {
            $b = \array_shift($bytes);
            $n |= ($b & 0x7f) << $shift;
            $shift += 7;
        }

        return ($n >> 1) ^ -($n & 1);
    }

    public static function intBitsToFloat(string $bits): float
    {
        $float = \unpack("f", $bits);

        return (float) $float[1];
    }

    public static function longBitsToDouble(string $bits): float
    {
        $double = \unpack("d", $bits);

        return (float) $double[1];
    }

    public function __construct(
        private readonly IO $io,
    ) {
    }

    private function nextByte(): string
    {
        return $this->read(1);
    }

    public function readNull(): null
    {
        return null;
    }

    public function readBoolean(): bool
    {
        return 1 === \ord($this->nextByte());
    }

    public function readInt(): int
    {
        return $this->readLong();
    }

    public function readLong(): int
    {
        $byte = \ord($this->nextByte());
        $bytes = [$byte];

        while (0 !== ($byte & 0x80)) {
            $byte = \ord($this->nextByte());
            $bytes[] = $byte;
        }

        return self::decodeLongFromArray($bytes);
    }

    public function readFloat(): float
    {
        return self::intBitsToFloat($this->read(4));
    }

    public function readDouble(): float
    {
        return self::longBitsToDouble($this->read(8));
    }

    public function readString(): string
    {
        return $this->readBytes();
    }

    public function readBytes(): string
    {
        return $this->read($this->readLong());
    }

    public function read(int $len): string
    {
        return $this->io->read($len);
    }

    public function skipNull(): void
    {
    }

    public function skipBoolean(): void
    {
        $this->skip(1);
    }

    public function skipInt(): void
    {
        $this->skipLong();
    }

    public function skipLong(): void
    {
        $b = \ord($this->nextByte());

        while (0 === $b || 0 !== ($b & 0x80)) {
            $b = \ord($this->nextByte());
        }
    }

    public function skipFloat(): void
    {
        $this->skip(4);
    }

    public function skipDouble(): void
    {
        $this->skip(8);
    }

    public function skipBytes(): void
    {
        $this->skip($this->readLong());
    }

    public function skipString(): void
    {
        $this->skipBytes();
    }

    public function skipFixed(FixedSchema $writersSchema, self $decoder): void
    {
        $decoder->skip($writersSchema->size());
    }

    public function skipEnum(EnumSchema $writersSchema, self $decoder): void
    {
        $decoder->skipInt();
    }

    /**
     * @throws Exception
     */
    public function skipUnion(UnionSchema $writersSchema, self $decoder): void
    {
        $index = $decoder->readLong();
        Reader::skipData($writersSchema->schemaByIndex($index), $decoder);
    }

    public function skipRecord(RecordSchema $writersSchema, self $decoder): void
    {
        foreach ($writersSchema->fields as $field) {
            Reader::skipData($field->schema, $decoder);
        }
    }

    public function skipArray(ArraySchema $writersSchema, self $decoder): void
    {
        $blockCount = $decoder->readLong();

        while (0 !== $blockCount) {
            if ($blockCount < 0) {
                $decoder->skip($this->readLong());
            }

            for ($i = 0; $i < $blockCount; ++$i) {
                Reader::skipData($writersSchema->items, $decoder);
            }

            $blockCount = $decoder->readLong();
        }
    }

    public function skipMap(MapSchema $writersSchema, self $decoder): void
    {
        $blockCount = $decoder->readLong();

        while (0 !== $blockCount) {
            if ($blockCount < 0) {
                $decoder->skip($this->readLong());
            }

            for ($i = 0; $i < $blockCount; ++$i) {
                $decoder->skipString();
                Reader::skipData($writersSchema->values, $decoder);
            }

            $blockCount = $decoder->readLong();
        }
    }

    public function skip(int $len): void
    {
        $this->seek($len, IO::SEEK_CUR);
    }

    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }
}
