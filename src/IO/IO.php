<?php

declare(strict_types=1);

namespace UserQQ\Avro\IO;

interface IO
{
    public const READ_MODE = 'r';

    public const WRITE_MODE = 'w';

    public const SEEK_CUR = \SEEK_CUR;

    public const SEEK_SET = \SEEK_SET;

    public const SEEK_END = \SEEK_END;

    public function read(int $len): string;

    public function write(string $str): void;

    public function tell(): int;

    public function seek(int $offset, int $whence = self::SEEK_SET): bool;

    public function isEof(): bool;

    public function flush(): void;

    public function close(): void;
}
