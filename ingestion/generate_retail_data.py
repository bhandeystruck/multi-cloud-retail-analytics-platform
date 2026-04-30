"""
Retail data generator.

This module will generate realistic sample retail data for local development.

Why we need this:
- We do not want to depend on external APIs at the beginning.
- A repeatable data generator allows us to test ingestion, storage,
  warehouse loading, transformations, and APIs.
- Later, this can be replaced or extended with real source integrations.
"""

from __future__ import annotations


def main() -> None:
    """
    Entry point for the data generator.

    """

    print("Retail data generator placeholder.")


if __name__ == "__main__":
    main()