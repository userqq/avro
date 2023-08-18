<?php

declare(strict_types=1);

namespace UserQQ\Avro\IO;

final class StringBuffer implements IO, \Stringable
{
    private string $buffer = '';
    private int $position = 0;
    private bool $isClosed = false;

    public function __construct(string $str = '')
    {
        $this->buffer .= $str;
    }

    /**
     * @throws Exception
     */
    public function write(string $str): void
    {
        $this->checkClosed();

        $this->buffer .= $str;
        $this->position = \strlen($this->buffer);
    }

    /**
     * @throws Exception
     */
    public function read(int $len): string
    {
        $this->checkClosed();
        $read = '';
        for (
            $i = $this->position;
            $i < $this->position + $len;
            ++$i
        ) {
            if (!isset($this->buffer[$i])) {
                continue;
            }

            $read .= $this->buffer[$i];
        }

        if (\strlen($read) < $len) {
            $this->position = $this->length();
        } else {
            $this->position += $len;
        }

        return $read;
    }

    /**
     * @throws Exception
     */
    public function seek(int $offset, int $whence = self::SEEK_SET): bool
    {
        switch ($whence) {
            case self::SEEK_SET:
                if (0 > $offset) {
                    throw new Exception('Cannot seek before beginning of file.');
                }

                $this->position = $offset;

                break;
            case self::SEEK_CUR:
                if (0 > $this->position + $whence) {
                    throw new Exception('Cannot seek before beginning of file.');
                }

                $this->position += $offset;

                break;
            case self::SEEK_END:
                if (0 > $this->length() + $offset) {
                    throw new Exception('Cannot seek before beginning of file.');
                }

                $this->position = $this->length() + $offset;

                break;
            default:
                throw new Exception(\sprintf('Invalid seek whence %d', $whence));
        }

        return true;
    }

    public function tell(): int
    {
        return $this->position;
    }

    public function isEof(): bool
    {
        return $this->position >= $this->length();
    }

    public function flush(): void
    {
    }

    /**
     * @throws Exception
     */
    public function close(): void
    {
        $this->checkClosed();
        $this->isClosed = true;
    }

    /**
     * @throws Exception
     */
    private function checkClosed(): void
    {
        if ($this->isClosed) {
            throw new Exception('Buffer is closed');
        }
    }

    /**
     * @throws Exception
     */
    public function truncate(): bool
    {
        $this->checkClosed();
        $this->buffer = '';
        $this->position = 0;

        return true;
    }

    public function length(): int
    {
        return \strlen($this->buffer);
    }

    public function __toString(): string
    {
        return $this->buffer;
    }

    public function string(): string
    {
        return $this->buffer;
    }

    public function isClosed(): bool
    {
        return $this->isClosed;
    }
}
