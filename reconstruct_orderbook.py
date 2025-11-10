import argparse
import json
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


# Increase precision for price/quantity arithmetic
getcontext().prec = 28


PriceLevel = Tuple[Decimal, Decimal]


class OrderBook:
    """In-memory order book that can replay Binance depth diffs."""

    def __init__(self) -> None:
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}

    def load_snapshot(self, snapshot: dict) -> None:
        """Initialize the book from a snapshot payload."""
        self.bids = self._levels_to_dict(snapshot.get("bids", []))
        self.asks = self._levels_to_dict(snapshot.get("asks", []))

    def apply_diff(self, diff: dict) -> None:
        """Apply a single depthUpdate event to the order book."""
        for price_str, qty_str in diff.get("b", []):
            self._update_level(self.bids, price_str, qty_str)
        for price_str, qty_str in diff.get("a", []):
            self._update_level(self.asks, price_str, qty_str)

    def top_levels(self, depth: int = 10) -> Tuple[List[PriceLevel], List[PriceLevel]]:
        """Return sorted top-of-book levels for bids (desc) and asks (asc)."""
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:depth]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        return bids_sorted, asks_sorted

    @staticmethod
    def _levels_to_dict(levels: Iterable[Iterable[str]]) -> Dict[Decimal, Decimal]:
        result: Dict[Decimal, Decimal] = {}
        for price_str, qty_str in levels:
            price = Decimal(price_str)
            qty = Decimal(qty_str)
            if qty > 0:
                result[price] = qty
        return result

    @staticmethod
    def _update_level(book_side: Dict[Decimal, Decimal], price_str: str, qty_str: str) -> None:
        price = Decimal(price_str)
        qty = Decimal(qty_str)
        if qty == 0:
            book_side.pop(price, None)
        else:
            book_side[price] = qty


def replay_diffs(file_path: Path, depth: int, event_limit: int) -> OrderBook:
    """
    Rebuild the order book by replaying a JSONL file
    produced by BinanceOrderBookTracker.
    """
    order_book = OrderBook()
    events_applied = 0

    with file_path.open("r") as f:
        for line in f:
            if not line.strip():
                continue

            entry = json.loads(line)
            entry_type = entry.get("type")

            if entry_type == "snapshot":
                order_book.load_snapshot(entry["data"])
            elif entry_type == "diff":
                order_book.apply_diff(entry["data"])
                events_applied += 1
                if event_limit and events_applied >= event_limit:
                    break

    if events_applied == 0:
        raise RuntimeError("No diff events were applied. Ensure the file contains data.")

    return order_book


def format_levels(levels: List[PriceLevel]) -> List[str]:
    """Format price levels for console output."""
    formatted = []
    for price, qty in levels:
        formatted.append(f"{price.normalize():>15} | {qty.normalize():>15}")
    return formatted


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconstruct an order book from Binance diff snapshots.")
    parser.add_argument("--file", required=True, type=Path, help="Path to the JSONL file with snapshot and diff entries.")
    parser.add_argument("--depth", type=int, default=10, help="Number of levels to display per side (default: 10).")
    parser.add_argument("--event-limit", type=int, default=0, help="Limit of diff events to replay (0 = all).")
    args = parser.parse_args()

    order_book = replay_diffs(args.file, args.depth, args.event_limit)
    bids, asks = order_book.top_levels(args.depth)

    print(f"Reconstructed order book from {args.file}")
    print(f"Diff events applied: {'all' if args.event_limit == 0 else args.event_limit}")
    print()
    print("   ----- BIDS (price | quantity) -----")
    for line in format_levels(bids):
        print(line)
    print()
    print("   ----- ASKS (price | quantity) -----")
    for line in format_levels(asks):
        print(line)


if __name__ == "__main__":
    main()

