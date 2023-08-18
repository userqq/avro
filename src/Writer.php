<?php

declare(strict_types=1);

namespace UserQQ\Avro;

use UserQQ\Avro\Datum\DataException;
use UserQQ\Avro\Datum\Encoder;
use UserQQ\Avro\IO\Exception as IOException;
use UserQQ\Avro\IO\IO;
use UserQQ\Avro\IO\StringBuffer;
use UserQQ\Avro\Schema\Exception\TypeException;
use UserQQ\Avro\Schema\Metadata;

/**
 * @api
 */
final class Writer
{
    private function generateSyncMarker(): string
    {
        return \pack(
            "S8",
            \random_int(0, 0xffff),
            \random_int(0, 0xffff),
            \random_int(0, 0xffff),
            \random_int(0, 0xffff) | 0x4000,
            \random_int(0, 0xffff) | 0x8000,
            \random_int(0, 0xffff),
            \random_int(0, 0xffff),
            \random_int(0, 0xffff),
        );
    }

    private readonly Schema $schema;
    private readonly Encoder $encoder;
    private readonly StringBuffer $stringBuffer;
    private readonly Encoder $bufferEncoder;
    private int $blockCount = 0;
    private array $metadata = [];
    private Codec $codec;
    private string $syncMarker;

    /**
     * @throws Exception
     * @throws DataException
     * @throws IOException
     */
    public function __construct(
        private readonly IO $io,
        ?Schema $schema = null,
        ?Codec $codec = null,
    ) {
        $this->encoder = new Encoder($this->io);
        $this->stringBuffer = new StringBuffer();
        $this->bufferEncoder = new Encoder($this->stringBuffer);

        if (null === $schema) {
            $reader = new Reader($io);
            $this->syncMarker = $reader->syncMarker;
            if (null !== $codec && $codec !== $reader->codec) {
                throw new DataException(\sprintf('Passed codec %s does not match file codec %s', $codec->value, $reader->codec->value));
            }
            $this->codec = $reader->codec;
            !(Codec::DEFLATE === $this->codec && !\function_exists('gzinflate'))
                ?: throw new DataException('"gzinflate" function not available, "zlib" extension required.');
            !(Codec::SNAPPY === $this->codec && !\function_exists('snappy_compress'))
                ?: throw new DataException('"snappy_compress" function not available, "snappy" extension required.');
            $this->metadata = $reader->metadata;
            $this->schema = $reader->schema;
            $this->seek(0, IO::SEEK_END);
        } else {
            $this->codec = $codec ?? Codec::NULL;
            !(Codec::DEFLATE === $this->codec && !\function_exists('gzinflate'))
                ?: throw new DataException('"gzinflate" function not available, "zlib" extension required.');
            !(Codec::SNAPPY === $this->codec && !\function_exists('snappy_compress'))
                ?: throw new DataException('"snappy_compress" function not available, "snappy" extension required.');
            $this->schema = $schema;
            $this->syncMarker = self::generateSyncMarker();
            $this->metadata[Metadata::SCHEMA_ATTRIBUTE] = (string) $schema;
            $this->metadata[Metadata::CODEC_ATTRIBUTE] = $this->codec->value;
            $this->writeHeader();
        }
    }

    /**
     * @throws Exception
     * @throws IOException
     * @throws TypeException
     */
    public function append(mixed $datum): void
    {
        $this->schema->write($datum, $this->bufferEncoder);
        ++$this->blockCount;

        if ($this->stringBuffer->length() >= Metadata::SYNC_INTERVAL) {
            $this->writeBlock();
        }
    }

    /**
     * @throws IOException
     */
    public function close(): void
    {
        $this->flush();

        $this->io->close();
    }

    /**
     * @throws IOException
     */
    private function flush(): void
    {
        $this->writeBlock();

        $this->io->flush();
    }

    /**
     * @throws IOException
     */
    private function writeBlock(): void
    {
        if ($this->blockCount > 0) {
            $this->encoder->writeLong($this->blockCount);
            $payload = (string) $this->stringBuffer;

            if (Codec::DEFLATE === $this->codec) {
                $payload = \gzdeflate($payload);
            } elseif (Codec::SNAPPY === $this->codec) {
                $crc32 = \pack('N', \crc32($payload));
                $payload = \snappy_compress($payload) . $crc32;
            }

            $this->encoder->writeLong(\strlen($payload));
            $this->write($payload);
            $this->write($this->syncMarker);
            $this->stringBuffer->truncate();
            $this->blockCount = 0;
        }
    }

    /**
     * @throws Exception
     * @throws TypeException
     */
    private function writeHeader(): void
    {
        $this->write(Metadata::magic());
        Metadata::schema()->write($this->metadata, $this->encoder);

        $this->write($this->syncMarker);
    }

    private function write(string $bytes): void
    {
        $this->io->write($bytes);
    }

    private function seek(int $offset, int $whence): void
    {
        $this->io->seek($offset, $whence);
    }
}
