<?php

declare(strict_types=1);

namespace UserQQ\Avro\Datum;

use UserQQ\Avro\IO\IO;

final class Encoder
{
    public function __construct(
        private readonly IO $io,
    ) {
    }

    public function writeNull(mixed $datum): null
    {
        return null;
    }

    public function writeBoolean(bool $datum): void
    {
        $byte = $datum ? \chr(1) : \chr(0);
        $this->write($byte);
    }

    public function writeInt(int $datum): void
    {
        $this->writeLong($datum);
    }

    public function writeLong(int $n): void
    {
        $n = ($n << 1) ^ ($n >> 63);

        $str = "";
        while (0 !== ($n & ~0x7f)) {
            $str .= \chr(($n & 0x7f) | 0x80);
            $n >>= 7;
        }

        $this->write($str . \chr($n));
    }

    public function writeFloat(float $datum): void
    {
        $this->write(\pack('g', $datum));
    }

    public function writeDouble(float $datum): void
    {
        $this->write(\pack('e', $datum));
    }

    public function writeString(string $str): void
    {
        $this->writeBytes($str);
    }

    public function writeBytes(string $bytes): void
    {
        $this->writeLong(\strlen($bytes));
        $this->write($bytes);
    }

    public function write(string $datum): void
    {
        $this->io->write($datum);
    }
}
