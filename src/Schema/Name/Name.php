<?php

declare(strict_types=1);

namespace UserQQ\Avro\Schema\Name;

use UserQQ\Avro\Schema\Exception\ParseException;

final class Name implements \Stringable
{
    private const NAME_SEPARATOR = '.';
    private const NAME_REGEXP = '/^[A-Za-z_]\w*$/';

    public readonly string $name;
    public readonly ?string $namespace;
    public readonly string $fullname;
    public readonly string $qualifiedName;

    /**
     * @throws ParseException
     */
    public function __construct(string $name, ?string $namespace = null, ?string $defaultNamespace = null)
    {
        if (!$this->isWellFormedName($name)) {
            throw new ParseException('Field requires a "name" attribute');
        }

        if (\strpos($name, self::NAME_SEPARATOR) && $this->checkNamespaceNames($name)) {
            $this->fullname = $name;
        } elseif (0 === \preg_match(self::NAME_REGEXP, $name)) {
            throw new ParseException(\sprintf('Invalid name "%s"', $name));
        } elseif (!\is_null($namespace)) {
            $this->fullname = $this->parseFullname($name, $namespace);
        } elseif (!\is_null($defaultNamespace)) {
            $this->fullname = $this->parseFullname($name, $defaultNamespace);
        } else {
            $this->fullname = $name;
        }

        [$this->name, $this->namespace] = $this->extractNamespace($this->fullname);
        if (!$this->isWellFormedName($name)) {
            throw new ParseException('Field requires a "name" attribute');
        }

        $this->qualifiedName = \is_null($this->namespace) || $this->namespace === $defaultNamespace
            ? $this->name
            : $this->fullname;
    }

    /**
     * @return list{string, string|null}
     */
    private function extractNamespace(string $name, ?string $namespace = null): array
    {
        $parts = \explode(self::NAME_SEPARATOR, $name);
        if (\count($parts) > 1) {
            $name = \array_pop($parts);
            $namespace = \implode(self::NAME_SEPARATOR, $parts);
        }

        return [$name, $namespace];
    }

    /**
     * @throws ParseException
     */
    private function checkNamespaceNames(string $namespace): bool
    {
        foreach (\explode(self::NAME_SEPARATOR, $namespace) as $n) {
            if ('' === $n || 0 === \preg_match(self::NAME_REGEXP, $n)) {
                throw new ParseException(\sprintf('Invalid name "%s"', $n));
            }
        }

        return true;
    }

    /**
     * @throws ParseException
     */
    private function parseFullname(string $name, string $namespace): string
    {
        if ('' === $namespace) {
            throw new ParseException('Namespace must be a non-empty string.');
        }

        $this->checkNamespaceNames($namespace);

        return $namespace . '.' . $name;
    }

    private function isWellFormedName(string $name): bool
    {
        return '' !== $name && \preg_match(self::NAME_REGEXP, $name);
    }

    public function __toString(): string
    {
        return $this->fullname;
    }
}
