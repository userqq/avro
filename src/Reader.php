<?php

declare(strict_types=1);

namespace UserQQ\Avro;

use UserQQ\Avro\Datum\DataException;
use UserQQ\Avro\Datum\Decoder;
use UserQQ\Avro\Datum\SchemaMatchException;
use UserQQ\Avro\IO\Exception as IOException;
use UserQQ\Avro\IO\IO;
use UserQQ\Avro\IO\StringBuffer;
use UserQQ\Avro\Schema\Exception\ParseException;
use UserQQ\Avro\Schema\Metadata;

/**
 * @api
 * @template TKey of int
 * @template TValue of mixed
 * @template-implements \IteratorAggregate<\Generator<int, mixed, mixed, void>>
 */
final class Reader implements \IteratorAggregate
{
    private readonly Datum\Reader $datumReader;
    private readonly Decoder $decoder;
    public readonly string $syncMarker;
    public readonly array $metadata;
    public readonly Codec $codec;
    public readonly Schema $schema;
    private int $blockCount;

    /**
     * @throws Exception
     * @throws DataException
     * @throws ParseException
     * @throws SchemaMatchException
     */
    public function __construct(
        private readonly IO $io,
    ) {
        $this->datumReader = new Datum\Reader();
        $this->decoder = new Decoder($this->io);
        [$this->metadata, $this->syncMarker] = $this->readHeader();

        $this->codec = !isset($this->metadata[Metadata::CODEC_ATTRIBUTE])
            ? Codec::NULL
            : (
                Codec::tryFrom($this->metadata[Metadata::CODEC_ATTRIBUTE])
                    ?: throw new DataException('Uknown codec: ', $this->metadata[Metadata::CODEC_ATTRIBUTE])
            );

        !(Codec::DEFLATE === $this->codec && !\function_exists('gzinflate'))
            ?: throw new DataException('"gzinflate" function not available, "zlib" extension required.');
        !(Codec::SNAPPY === $this->codec && !\function_exists('snappy_compress'))
            ?: throw new DataException('"snappy_compress" function not available, "snappy" extension required.');

        $this->blockCount = 0;
        $this->schema = Schema::parse($this->metadata[Metadata::SCHEMA_ATTRIBUTE]);

        // FIXME: Seems unsanitary to set writers_schema here.
        // Can't constructor take it as an argument?
        $this->datumReader->setWritersSchema($this->schema);
    }

    /**
     * @throws Exception
     * @throws DataException
     * @throws SchemaMatchException
     */
    private function readHeader(): array
    {
        $this->seek(0, IO::SEEK_SET);

        $magic = $this->read(Metadata::magicSize());

        if (\strlen($magic) < Metadata::magicSize()) {
            throw new DataException('Not an Avro data file: shorter than the Avro magic block');
        }

        if (Metadata::magic() !== $magic) {
            throw new DataException(\sprintf('Not an Avro data file: %s does not match %s', $magic, Metadata::magic()));
        }

        return [
            $this->datumReader->readData(Metadata::schema(), Metadata::schema(), $this->decoder),
            $this->read(Metadata::SYNC_SIZE),
        ];
    }

    /**
     * @throws DataException
     * @throws IOException
     */
    public function data(): array
    {
        return \iterator_to_array($this);
    }

    /**
     * @throws DataException
     * @throws IOException
     * @psalm-return \Generator<int, mixed, mixed, void>
     */
    public function getIterator(): \Generator
    {
        while (true) {
            if (0 === $this->blockCount) {
                if ($this->isEof()) {
                    break;
                }

                if ($this->skipSync() && $this->isEof()) {
                    break;
                }

                $decoder = $this->applyCodec($this->decoder, $this->codec);
            }

            /** @psalm-suppress PossiblyUndefinedVariable */
            yield $this->datumReader->read($decoder);

            --$this->blockCount;
        }
    }

    /**
     * @throws DataException
     * @throws IOException
     */
    private function applyCodec(Decoder $decoder, Codec $codec): Decoder
    {
        $length = $this->readBlockHeader();

        if (Codec::DEFLATE === $codec) {
            $compressed = $decoder->read($length);
            $datum = \gzinflate($compressed);
            $decoder = new Decoder(new StringBuffer($datum));
        } elseif (Codec::SNAPPY === $codec) {
            $compressed = $decoder->read($length - 4);
            $datum = \snappy_uncompress($compressed);
            $crc32 = \unpack('N', $decoder->read(4));

            if ($crc32[1] !== \crc32($datum)) {
                throw new DataException('Invalid CRC32 checksum.');
            }

            $decoder = new Decoder(new StringBuffer($datum));
        }

        return $decoder;
    }

    public function close(): void
    {
        $this->io->close();
    }

    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }

    private function read(int $len): string
    {
        return $this->io->read($len);
    }

    private function isEof(): bool
    {
        return $this->io->isEof();
    }

    private function skipSync(): bool
    {
        $proposedSyncMarker = $this->read(Metadata::SYNC_SIZE);

        if ($proposedSyncMarker !== $this->syncMarker) {
            $this->seek(-Metadata::SYNC_SIZE, IO::SEEK_CUR);

            return false;
        }

        return true;
    }

    private function readBlockHeader(): int
    {
        $this->blockCount = $this->decoder->readLong();

        return $this->decoder->readLong();
    }
}
